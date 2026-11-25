//! Native Arrow/Parquet decoding with lossless versioned physical log frames.
use std::collections::HashSet;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use arrow_array::{
    Array, ArrayRef, Int64Array, LargeListArray, ListArray, MapArray, RecordBatch, StringArray,
    StructArray,
};
use arrow_json::writer::LineDelimited;
use arrow_json::WriterBuilder;
use arrow_schema::{DataType, Schema, TimeUnit};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::file::statistics::Statistics;

use crate::encoding::{frame, MAX_LINE_BYTES, RESERVED};
use crate::wire::{Entry, LabelPair, Timestamp};

pub struct Source {
    pub path: PathBuf,
    pub name: String,
    pub metadata: ArrowReaderMetadata,
    pub offsets: Vec<u64>,
    pub rows: u64,
    pub min_ns: i64,
    pub max_ns: i64,
}

fn unique_index(schema: &Schema, name: &str) -> Result<usize> {
    let matches: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name() == name)
        .collect();
    ensure!(matches.len() == 1, "exactly one {name} field is required");
    Ok(matches[0].0)
}

fn timestamp_scale(ty: &DataType) -> Result<i64> {
    Ok(match ty {
        DataType::Int64 | DataType::Timestamp(TimeUnit::Nanosecond, _) => 1,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000,
        DataType::Timestamp(TimeUnit::Millisecond, _) => 1_000_000,
        DataType::Timestamp(TimeUnit::Second, _) => 1_000_000_000,
        _ => bail!("unsupported Timestamp type {ty}; expected timestamp or int64 nanoseconds"),
    })
}

fn checked_ns(value: i64, scale: i64) -> Result<i64> {
    value
        .checked_mul(scale)
        .context("Timestamp overflows int64 nanoseconds")
}

pub fn timestamps(column: &ArrayRef) -> Result<Vec<i64>> {
    let scale = timestamp_scale(column.data_type())?;
    ensure!(column.null_count() == 0, "null Timestamp");
    let ints = arrow_cast::cast(column, &DataType::Int64)?;
    ints.as_any()
        .downcast_ref::<Int64Array>()
        .context("Timestamp cast failed")?
        .values()
        .iter()
        .map(|v| checked_ns(*v, scale))
        .collect()
}

impl Source {
    pub fn open(path: PathBuf) -> Result<Self> {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("non-UTF8 source filename")?
            .to_owned();
        let metadata = ArrowReaderMetadata::load(&File::open(&path)?, ArrowReaderOptions::new())?;
        let schema = metadata.schema();
        let ts_index = unique_index(schema, "Timestamp")?;
        let scale = timestamp_scale(schema.field(ts_index).data_type())?;
        for name in ["Body", "ServiceName", "SeverityText"] {
            let index = unique_index(schema, name)?;
            ensure!(
                string_type(schema.field(index).data_type()),
                "{name} must be a string column"
            );
        }
        let mut names = HashSet::new();
        for field in schema.fields() {
            ensure!(
                names.insert(field.name()),
                "duplicate source field {}",
                field.name()
            );
            ensure!(
                !RESERVED.contains(&field.name().as_str()),
                "reserved source field {}",
                field.name()
            );
        }
        let mut offsets = Vec::new();
        let (mut rows, mut min_ns, mut max_ns) = (0, i64::MAX, i64::MIN);
        for (rg_index, rg) in metadata.metadata().row_groups().iter().enumerate() {
            offsets.push(rows);
            rows += u64::try_from(rg.num_rows())?;
            if rg.num_rows() == 0 {
                continue;
            }
            let stats = rg
                .columns()
                .iter()
                .find(|c| c.column_path().parts() == ["Timestamp"])
                .and_then(|c| c.statistics());
            let bounds = match stats {
                Some(Statistics::Int64(s)) if s.null_count_opt() == Some(0) => {
                    s.min_opt().zip(s.max_opt()).map(|(lo, hi)| (*lo, *hi))
                }
                _ => None,
            };
            if let Some((lo, hi)) = bounds {
                min_ns = min_ns.min(checked_ns(lo, scale)?);
                max_ns = max_ns.max(checked_ns(hi, scale)?);
            } else {
                // Statistics may be missing: scan Timestamp ONLY, before any
                // HTTP workers start. Do not trust a default min/max or null.
                let projection = ProjectionMask::roots(
                    metadata.metadata().file_metadata().schema_descr(),
                    [ts_index],
                );
                let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                    File::open(&path)?,
                    metadata.clone(),
                )
                .with_projection(projection)
                .with_row_groups(vec![rg_index])
                .with_batch_size(65536)
                .build()?;
                for batch in reader {
                    for ts in timestamps(batch?.column(0))? {
                        min_ns = min_ns.min(ts);
                        max_ns = max_ns.max(ts);
                    }
                }
            }
        }
        // Scan only Body before any HTTP workers. A UTF-8 byte expands to at
        // most six JSON bytes; small strings need no per-row serialization.
        // Longest global file offset makes the overhead bound conservative.
        let overhead = frame("", &format!("{name}:{}", rows.saturating_sub(1)))?.len();
        let threshold = (MAX_LINE_BYTES - overhead) / 6;
        let projection = ProjectionMask::roots(
            metadata.metadata().file_metadata().schema_descr(),
            [unique_index(schema, "Body")?],
        );
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
            File::open(&path)?,
            metadata.clone(),
        )
        .with_projection(projection)
        .with_batch_size(65536)
        .build()?;
        let mut offset = 0u64;
        for batch in reader {
            let batch = batch?;
            let body = strings(&batch, "Body")?;
            let body = body.as_any().downcast_ref::<StringArray>().unwrap();
            for (index, value) in body.iter().flatten().enumerate() {
                if value.len() > threshold {
                    frame(value, &format!("{name}:{}", offset + index as u64))?;
                }
            }
            offset += batch.num_rows() as u64;
        }
        ensure!(offset == rows, "Body row count changed during preflight");
        Ok(Self {
            path,
            name,
            metadata,
            offsets,
            rows,
            min_ns,
            max_ns,
        })
    }
}

fn string_type(ty: &DataType) -> bool {
    match ty {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, value) => string_type(value),
        _ => false,
    }
}

fn strings(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    let index = unique_index(&batch.schema(), name)?;
    let array = batch.column(index);
    ensure!(
        string_type(array.data_type()) && array.null_count() == 0,
        "{name} must contain non-null strings"
    );
    Ok(arrow_cast::cast(array, &DataType::Utf8)?)
}

fn validate_maps(array: &ArrayRef) -> Result<()> {
    match array.data_type() {
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            ensure!(
                string_type(map.keys().data_type()),
                "metadata map keys must be strings"
            );
            let keys = arrow_cast::cast(map.keys(), &DataType::Utf8)?;
            let keys = keys.as_any().downcast_ref::<StringArray>().unwrap();
            for row in 0..map.len() {
                if map.is_null(row) {
                    continue;
                }
                let offsets = map.value_offsets();
                let mut seen = HashSet::new();
                for index in offsets[row] as usize..offsets[row + 1] as usize {
                    ensure!(
                        !keys.is_null(index) && seen.insert(keys.value(index)),
                        "null or duplicate metadata map key"
                    );
                }
            }
            validate_maps(map.values())?;
        }
        DataType::Struct(fields) => {
            let mut names = HashSet::new();
            ensure!(
                fields.iter().all(|field| names.insert(field.name())),
                "duplicate metadata struct field"
            );
            for column in array
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .columns()
            {
                validate_maps(column)?;
            }
        }
        DataType::List(_) => {
            validate_maps(array.as_any().downcast_ref::<ListArray>().unwrap().values())?
        }
        DataType::LargeList(_) => validate_maps(
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .values(),
        )?,
        DataType::Float32 | DataType::Float64 => {
            let floats = arrow_cast::cast(array, &DataType::Float64)?;
            let floats = floats
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .unwrap();
            ensure!(
                floats.iter().flatten().all(f64::is_finite),
                "non-finite metadata float; refusing lossy JSON conversion"
            );
        }
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => {}
        other => bail!("unsupported metadata type {other}; refusing a lossy conversion"),
    }
    Ok(())
}

// Python's Arrow conversion widens f32 to f64 before JSON serialization.
// Preserve that exact value, including inside nested metadata, rather than
// serializing shortest-f32 text and accidentally changing its f64 value.
fn metadata_json_type(ty: &DataType) -> DataType {
    let field = |f: &Arc<arrow_schema::Field>| {
        Arc::new(
            f.as_ref()
                .clone()
                .with_data_type(metadata_json_type(f.data_type())),
        )
    };
    match ty {
        DataType::Float32 => DataType::Float64,
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        DataType::List(f) => DataType::List(field(f)),
        DataType::LargeList(f) => DataType::LargeList(field(f)),
        DataType::Map(f, sorted) => DataType::Map(field(f), *sorted),
        other => other.clone(),
    }
}

pub struct EncodedRow {
    pub labels: String,
    pub entry: Entry,
    pub timestamp_ns: i64,
}

pub fn convert(batch: &RecordBatch, filename: &str, first_row: u64) -> Result<Vec<EncodedRow>> {
    let times = timestamps(batch.column(unique_index(&batch.schema(), "Timestamp")?))?;
    let body = strings(batch, "Body")?;
    let service = strings(batch, "ServiceName")?;
    let severity = strings(batch, "SeverityText")?;
    let body = body.as_any().downcast_ref::<StringArray>().unwrap();
    let service = service.as_any().downcast_ref::<StringArray>().unwrap();
    let severity = severity.as_any().downcast_ref::<StringArray>().unwrap();

    // Arrow-native batch serialization only for metadata. Body is encoded
    // separately into its canonical physical frame, then sent inside protobuf.
    // Nested metadata remains JSON-valued strings like the Python reference.
    let schema = batch.schema();
    let selected: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            ![
                "Timestamp",
                "TimestampTime",
                "Body",
                "ServiceName",
                "SeverityText",
            ]
            .contains(&field.name().as_str())
        })
        .collect();
    let mut metadata_rows = vec![Vec::new(); batch.num_rows()];
    if !selected.is_empty() {
        let fields = selected
            .iter()
            .map(|(_, f)| {
                Arc::new(
                    f.as_ref()
                        .clone()
                        .with_data_type(metadata_json_type(f.data_type())),
                )
            })
            .collect::<Vec<_>>();
        let mut columns = Vec::with_capacity(selected.len());
        for ((index, _), field) in selected.iter().zip(&fields) {
            let column = batch.column(*index);
            validate_maps(column)?;
            columns.push(arrow_cast::cast(column, field.data_type())?);
        }
        let metadata_batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(Vec::new());
        writer.write(&metadata_batch)?;
        writer.finish()?;
        let json = writer.into_inner();
        let mut lines = json.split(|c| *c == b'\n').filter(|line| !line.is_empty());
        for labels in &mut metadata_rows {
            let object: serde_json::Map<String, serde_json::Value> =
                serde_json::from_slice(lines.next().context("missing metadata row")?)?;
            for (name, value) in object {
                if value.is_null() {
                    continue;
                }
                let value = match value {
                    serde_json::Value::String(s) => s,
                    value => serde_json::to_string(&value)?,
                };
                labels.push(LabelPair { name, value });
            }
        }
        ensure!(lines.next().is_none(), "extra metadata rows");
    }
    let mut result = Vec::with_capacity(batch.num_rows());
    for (row, (ts, mut metadata)) in times.into_iter().zip(metadata_rows).enumerate() {
        let identity = format!("{filename}:{}", first_row + row as u64);
        metadata.push(LabelPair {
            name: "sb_row_id".into(),
            value: identity.clone(),
        });
        let labels = format!(
            "{{dataset=\"otel_logs\",service_name={},severity_text={}}}",
            serde_json::to_string(service.value(row))?,
            serde_json::to_string(severity.value(row))?
        );
        result.push(EncodedRow {
            labels,
            timestamp_ns: ts,
            entry: Entry {
                timestamp: Some(Timestamp::from_ns(ts)),
                line: frame(body.value(row), &identity)?,
                structured_metadata: metadata,
            },
        });
    }
    Ok(result)
}
