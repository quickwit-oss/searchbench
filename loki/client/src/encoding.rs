//! Versioned physical line contract, byte-identical to Python encoding.py.
use anyhow::{ensure, Result};
use serde::Serialize;

pub const FLAG: &str = "searchbench-loki-json-frame-v1";
pub const MAX_LINE_BYTES: usize = 1024 * 1024;
pub const MAX_REQUEST_BYTES: usize = 3 * 1024 * 1024;
pub const RESERVED: [&str; 4] = ["sb_row_id", "sb_frame", "sb_window", "sb_original"];

#[derive(Serialize)]
pub struct Descriptor {
    name: &'static str,
    version: u8,
    identity: &'static str,
}

pub fn descriptor() -> Descriptor {
    Descriptor {
        name: "searchbench-loki-json-frame",
        version: 1,
        identity: "source-file-row-v1",
    }
}

#[derive(Serialize)]
struct Frame<'a> {
    v: u8,
    id: &'a str,
    body: &'a str,
}

pub fn frame(body: &str, identity: &str) -> Result<String> {
    ensure!(!identity.is_empty(), "empty source identity");
    let line = serde_json::to_string(&Frame {
        v: 1,
        id: identity,
        body,
    })?;
    ensure!(
        line.len() <= MAX_LINE_BYTES,
        "physical frame exceeds the 1 MiB Loki line cap; refusing truncation"
    );
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_frame_and_physical_cap() {
        assert_eq!(
            frame("\0\n雪\"\\", "p:1").unwrap(),
            "{\"v\":1,\"id\":\"p:1\",\"body\":\"\\u0000\\n雪\\\"\\\\\"}"
        );
        let overhead = frame("", "p:1").unwrap().len();
        assert_eq!(
            frame(&"x".repeat(MAX_LINE_BYTES - overhead), "p:1")
                .unwrap()
                .len(),
            MAX_LINE_BYTES
        );
        assert!(frame(&"x".repeat(MAX_LINE_BYTES - overhead + 1), "p:1").is_err());
        assert!(frame(&"\0".repeat(180_000), "p:1").is_err());
    }
}
