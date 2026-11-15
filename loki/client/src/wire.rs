//! Wire-compatible subset of grafana/loki v3.7.9 pkg/push/push.proto:
//! https://github.com/grafana/loki/blob/v3.7.9/pkg/push/push.proto
//! Omitted hash/format/parsed fields are optional and must not be populated by
//! this client. Defining the wire fields directly avoids a protoc build step.

#[derive(Clone, PartialEq, prost::Message)]
pub struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    pub streams: Vec<Stream>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Stream {
    #[prost(string, tag = "1")]
    pub labels: String,
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<Entry>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Entry {
    #[prost(message, optional, tag = "1")]
    pub timestamp: Option<Timestamp>,
    #[prost(string, tag = "2")]
    pub line: String,
    #[prost(message, repeated, tag = "3")]
    pub structured_metadata: Vec<LabelPair>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Timestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct LabelPair {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

impl Timestamp {
    pub fn from_ns(ns: i64) -> Self {
        Self {
            seconds: ns.div_euclid(1_000_000_000),
            nanos: ns.rem_euclid(1_000_000_000) as i32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn timestamp_is_lossless_including_negative_epoch() {
        for ns in [-1, 0, 1, 1758585600000000001, i64::MAX] {
            let ts = Timestamp::from_ns(ns);
            assert_eq!(
                ts.seconds as i128 * 1_000_000_000 + ts.nanos as i128,
                ns as i128
            );
            assert!((0..1_000_000_000).contains(&ts.nanos));
        }
    }

    #[test]
    fn protobuf_raw_snappy_round_trip() {
        let request = PushRequest {
            streams: vec![Stream {
                labels: "{dataset=\"otel_logs\"}".into(),
                entries: vec![Entry {
                    timestamp: Some(Timestamp::from_ns(1758585600000000001)),
                    line: "ERROR\nwith unicode é".into(),
                    structured_metadata: vec![LabelPair {
                        name: "sb_row_id".into(),
                        value: "part_0.parquet:1".into(),
                    }],
                }],
            }],
        };
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .unwrap();
        let bytes = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        assert_eq!(PushRequest::decode(bytes.as_slice()).unwrap(), request);
    }
}
