//! Parquet row-group workers -> bounded queue -> persistent HTTP senders.
//! The only stdout output is a JSON manifest, and only after all pushes succeed.
//! No retries: a timeout/5xx/partial acceptance must not become a duplicate load.
mod encoding;
mod input;
mod wire;

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use crossbeam_channel::{bounded, SendTimeoutError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prost::Message;
use serde::Serialize;

use input::{convert, Source};
use wire::{Entry, PushRequest, Stream};

#[derive(Parser, Debug)]
#[command(about = "Parquet -> Loki raw-Snappy protobuf ingestion (no automatic retries)")]
struct Args {
    #[arg(long = "file", required = true)]
    files: Vec<PathBuf>,
    // Required handshake: old/raw binaries reject these unknown flags before
    // pushing, and this binary refuses missing/unknown encodings.
    #[arg(long)]
    line_encoding: String,
    #[arg(long)]
    destination_id: String,
    #[arg(long, default_value = "http://127.0.0.1:3100")]
    url: String,
    #[arg(long, default_value_t = 8)]
    threads: usize,
    #[arg(long, default_value_t = 8)]
    http_workers: usize,
    #[arg(long, default_value_t = 5000)]
    batch_size: usize,
    #[arg(long, default_value_t = 8)]
    queue_depth: usize,
    // Stay below Loki's default 4 MiB internal gRPC receive limit, with room
    // for distributor/ingester framing and labels added server-side.
    #[arg(long, default_value_t = 3 * 1024 * 1024)]
    max_request_bytes: usize,
    #[arg(long, default_value_t = 120)]
    timeout_secs: u64,
}

struct Work {
    source: Arc<Source>,
    row_group: usize,
}

struct Payload {
    bytes: Vec<u8>,
    rows: u64,
    raw_bytes: usize,
}

#[derive(Default)]
struct ReadStats {
    rows: u64,
    min_ns: Option<i64>,
    max_ns: Option<i64>,
}

impl ReadStats {
    fn add(&mut self, ts: i64) {
        self.rows += 1;
        self.min_ns = Some(self.min_ns.map_or(ts, |v| v.min(ts)));
        self.max_ns = Some(self.max_ns.map_or(ts, |v| v.max(ts)));
    }
}

#[derive(Serialize)]
struct Summary {
    rows: u64,
    min_timestamp_ns: i64,
    max_timestamp_ns: i64,
    files: usize,
    row_groups: usize,
    readers: usize,
    http_workers: usize,
    batch_rows: usize,
    queue_depth: usize,
    max_request_bytes: usize,
    requests: u64,
    protobuf_bytes: u64,
    compressed_bytes: u64,
    elapsed_seconds: f64,
    client: &'static str,
    wire_format: &'static str,
    encoding: encoding::Descriptor,
    destination_id: String,
}

fn payload(streams: BTreeMap<String, Vec<Entry>>, max_bytes: usize) -> Result<Payload> {
    let rows = streams.values().map(|v| v.len() as u64).sum();
    let streams = streams
        .into_iter()
        .map(|(labels, mut entries)| {
            entries.sort_by_key(|e| e.timestamp.as_ref().map(|t| (t.seconds, t.nanos)));
            Stream { labels, entries }
        })
        .collect();
    let request = PushRequest { streams };
    let raw = request.encode_to_vec();
    ensure!(
        raw.len() <= max_bytes,
        "protobuf exceeds configured request byte bound"
    );
    let bytes = snap::raw::Encoder::new().compress_vec(&raw)?;
    Ok(Payload {
        bytes,
        rows,
        raw_bytes: raw.len(),
    })
}

fn enqueue(
    tx: &crossbeam_channel::Sender<Payload>,
    mut value: Payload,
    cancel: &AtomicBool,
) -> Result<()> {
    loop {
        ensure!(
            !cancel.load(Ordering::Acquire),
            "cancelled after another worker failed"
        );
        match tx.send_timeout(value, Duration::from_millis(100)) {
            Ok(()) => return Ok(()),
            Err(SendTimeoutError::Timeout(v)) => value = v,
            Err(SendTimeoutError::Disconnected(_)) => bail!("all HTTP senders stopped"),
        }
    }
}

fn read_work(
    work: &Work,
    args: &Args,
    tx: &crossbeam_channel::Sender<Payload>,
    cancel: &AtomicBool,
    stats: &mut ReadStats,
) -> Result<()> {
    let source = &work.source;
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
        File::open(&source.path)?,
        source.metadata.clone(),
    )
    .with_row_groups(vec![work.row_group])
    .with_batch_size(args.batch_size)
    .build()?;
    let mut offset = source.offsets[work.row_group];
    for batch in reader {
        ensure!(
            !cancel.load(Ordering::Acquire),
            "cancelled after another worker failed"
        );
        let batch = batch?;
        let converted = convert(&batch, &source.name, offset)?;
        let mut streams: BTreeMap<String, Vec<Entry>> = BTreeMap::new();
        let mut estimated = 0usize;
        for row in converted {
            // Conservative bound includes per-entry framing AND repeats the
            // stream-label overhead per entry. Actual encoded_len is checked
            // before enqueueing. One oversize row fails, never gets truncated.
            let cost = row.entry.encoded_len() + row.labels.len() + 64;
            ensure!(
                cost <= args.max_request_bytes,
                "one row exceeds --max-request-bytes"
            );
            if estimated + cost > args.max_request_bytes && !streams.is_empty() {
                enqueue(
                    tx,
                    payload(std::mem::take(&mut streams), args.max_request_bytes)?,
                    cancel,
                )?;
                estimated = 0;
            }
            estimated += cost;
            streams.entry(row.labels).or_default().push(row.entry);
            stats.add(row.timestamp_ns);
        }
        if !streams.is_empty() {
            enqueue(tx, payload(streams, args.max_request_bytes)?, cancel)?;
        }
        offset += batch.num_rows() as u64;
    }
    let expected = work
        .source
        .metadata
        .metadata()
        .row_group(work.row_group)
        .num_rows() as u64;
    ensure!(
        offset - source.offsets[work.row_group] == expected,
        "source row-group size changed"
    );
    Ok(())
}

fn execute(args: Args) -> Result<Summary> {
    ensure!(
        args.line_encoding == encoding::FLAG,
        "unsupported line encoding; expected {}",
        encoding::FLAG
    );
    ensure!(
        !args.destination_id.is_empty(),
        "empty destination identity"
    );
    ensure!(
        args.threads > 0
            && args.http_workers > 0
            && args.batch_size > 0
            && args.queue_depth > 0
            && args.timeout_secs > 0,
        "worker counts, batch/queue sizes and timeout must be positive"
    );
    ensure!(
        (1024..=encoding::MAX_REQUEST_BYTES).contains(&args.max_request_bytes),
        "--max-request-bytes must be between 1024 and 3145728"
    );
    ensure!(
        args.url.starts_with("http://"),
        "this local benchmark client requires an explicit http:// endpoint"
    );
    let start = Instant::now();
    let mut work = Vec::new();
    let (mut expected_rows, mut minimum, mut maximum) = (0u64, i64::MAX, i64::MIN);
    let mut names = HashSet::new();
    // Complete corpus preflight before starting ANY network request. This is
    // also required when the binary is invoked without the Python harness.
    for path in &args.files {
        let source =
            Arc::new(Source::open(path.clone()).with_context(|| path.display().to_string())?);
        ensure!(
            names.insert(source.name.clone()),
            "duplicate basename {} would collide in sb_row_id",
            source.name
        );
        expected_rows += source.rows;
        if source.rows > 0 {
            minimum = minimum.min(source.min_ns);
            maximum = maximum.max(source.max_ns);
        }
        for row_group in 0..source.metadata.metadata().num_row_groups() {
            work.push(Work {
                source: source.clone(),
                row_group,
            });
        }
    }
    ensure!(expected_rows > 0, "empty corpus");
    ensure!(
        minimum >= 1_577_836_800_000_000_000,
        "timestamps predate the configured 2020 schema"
    );
    ensure!(
        maximum - minimum < 3_600_000_000_000,
        "corpus spans >=1h; a time-ordered loader is required"
    );
    let readers = args.threads.min(work.len());
    eprintln!("preflight: {} files, {} row groups, {expected_rows} rows; {readers} readers + {} HTTP senders", args.files.len(), work.len(), args.http_workers);

    let cancel = AtomicBool::new(false);
    let next = AtomicUsize::new(0);
    let (acked, requests, raw_bytes, compressed_bytes) = (
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
    );
    let (tx, rx) = bounded::<Payload>(args.queue_depth);
    let url = format!("{}/loki/api/v1/push", args.url.trim_end_matches('/'));
    let mut read_stats = Vec::new();
    let mut failures = Vec::new();
    std::thread::scope(|scope| {
        let mut senders = Vec::new();
        for _ in 0..args.http_workers {
            let (rx, url, cancel, acked, requests, raw_bytes, compressed_bytes) = (
                rx.clone(),
                &url,
                &cancel,
                &acked,
                &requests,
                &raw_bytes,
                &compressed_bytes,
            );
            let timeout = args.timeout_secs;
            senders.push(scope.spawn(move || {
                let result = (|| -> Result<()> {
                    let agent = ureq::AgentBuilder::new()
                        .timeout(Duration::from_secs(timeout))
                        .redirects(0)
                        .build();
                    for p in rx {
                        if cancel.load(Ordering::Acquire) {
                            break;
                        }
                        let response = agent
                            .post(url)
                            .set("Content-Type", "application/x-protobuf")
                            .send_bytes(&p.bytes);
                        match response {
                            Ok(response) if response.status() == 204 => {}
                            Ok(response) => bail!(
                                "unexpected HTTP {}; partial load must not be reused",
                                response.status()
                            ),
                            Err(ureq::Error::Status(code, response)) => {
                                let text = response.into_string().unwrap_or_default();
                                bail!(
                                    "HTTP {code}; no retry of a possibly partial push: {}",
                                    text.chars().take(1500).collect::<String>()
                                );
                            }
                            Err(error) => {
                                bail!("push failed; no retry of ambiguous acceptance: {error}")
                            }
                        }
                        acked.fetch_add(p.rows, Ordering::Relaxed);
                        requests.fetch_add(1, Ordering::Relaxed);
                        raw_bytes.fetch_add(p.raw_bytes as u64, Ordering::Relaxed);
                        compressed_bytes.fetch_add(p.bytes.len() as u64, Ordering::Relaxed);
                    }
                    Ok(())
                })();
                if result.is_err() {
                    cancel.store(true, Ordering::Release);
                }
                result
            }));
        }
        drop(rx);
        let mut producers = Vec::new();
        for _ in 0..readers {
            let (tx, work, next, cancel, args) = (tx.clone(), &work, &next, &cancel, &args);
            producers.push(scope.spawn(move || {
                let mut stats = ReadStats::default();
                let result = (|| -> Result<()> {
                    loop {
                        ensure!(
                            !cancel.load(Ordering::Acquire),
                            "cancelled after another worker failed"
                        );
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(job) = work.get(index) else {
                            break;
                        };
                        read_work(job, args, &tx, cancel, &mut stats)?;
                    }
                    Ok(())
                })();
                if result.is_err() {
                    cancel.store(true, Ordering::Release);
                }
                result.map(|_| stats)
            }));
        }
        drop(tx);
        for producer in producers {
            match producer.join() {
                Ok(Ok(stats)) => read_stats.push(stats),
                Ok(Err(error)) => failures.push(error),
                Err(_) => {
                    cancel.store(true, Ordering::Release);
                    failures.push(anyhow::anyhow!("Parquet worker panicked"));
                }
            }
        }
        for sender in senders {
            match sender.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error),
                Err(_) => failures.push(anyhow::anyhow!("HTTP worker panicked")),
            }
        }
    });
    if !failures.is_empty() {
        for failure in &failures {
            eprintln!("worker error: {failure:#}");
        }
        bail!(
            "load aborted; {} acknowledged rows, destination may be partial",
            acked.load(Ordering::Relaxed)
        );
    }
    let rows = acked.load(Ordering::Relaxed);
    ensure!(
        rows == expected_rows && read_stats.iter().map(|s| s.rows).sum::<u64>() == expected_rows,
        "source/acknowledged row count mismatch"
    );
    ensure!(
        read_stats.iter().filter_map(|s| s.min_ns).min() == Some(minimum)
            && read_stats.iter().filter_map(|s| s.max_ns).max() == Some(maximum),
        "source time bounds changed after preflight"
    );
    Ok(Summary {
        rows,
        min_timestamp_ns: minimum,
        max_timestamp_ns: maximum,
        files: args.files.len(),
        row_groups: work.len(),
        readers,
        http_workers: args.http_workers,
        batch_rows: args.batch_size,
        queue_depth: args.queue_depth,
        max_request_bytes: args.max_request_bytes,
        requests: requests.load(Ordering::Relaxed),
        protobuf_bytes: raw_bytes.load(Ordering::Relaxed),
        compressed_bytes: compressed_bytes.load(Ordering::Relaxed),
        elapsed_seconds: start.elapsed().as_secs_f64(),
        client: "rust",
        wire_format: "snappy-protobuf",
        encoding: encoding::descriptor(),
        destination_id: args.destination_id,
    })
}

fn main() -> Result<()> {
    let summary = execute(Args::parse())?;
    eprintln!(
        "complete: {} rows / {} pushes in {:.3}s",
        summary.rows, summary.requests, summary.elapsed_seconds
    );
    println!("{}", serde_json::to_string(&summary)?);
    Ok(())
}
