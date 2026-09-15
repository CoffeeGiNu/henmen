use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use crate::delegation::{application::LogEntry, ports::LogSink};

pub struct LogFile {
    path: PathBuf,
}

impl LogFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl LogSink for LogFile {
    fn append(&self, entry: &LogEntry) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut line: Vec<u8> = serde_json::to_vec(entry)?;
        line.push(b'\n');
        let mut file: File = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&line)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delegation::application::{Mode, Operation, SessionId, SessionStatus, Usage};

    fn sample(session_id: &str, usage: Option<Usage>) -> LogEntry {
        LogEntry {
            timestamp_start: 1_757_894_400_000,
            timestamp_end: 1_757_894_412_500,
            duration_milliseconds: 12_500,
            kind: Operation::Delegate,
            session_id: SessionId(session_id.to_string()),
            model: "gpt-5.6-luna".to_string(),
            effort: "xhigh".to_string(),
            mode: Mode::Inspect,
            status: SessionStatus::Completed,
            usage,
            worker_startup_milliseconds: Some(340),
        }
    }

    #[test]
    fn appends_one_line_per_record() -> anyhow::Result<()> {
        let root: std::path::PathBuf =
            std::env::temp_dir().join(format!("henmen-run-log-test-{}", std::process::id()));
        let log: LogFile = LogFile::new(root.join("logs").join("runs.jsonl"));

        log.append(&sample(
            "01K5CQXM8N7VZR3TFWJ0HB2YQD",
            Some(Usage {
                input_tokens: Some(120),
                cached_input_tokens: Some(64),
                output_tokens: Some(30),
            }),
        ))?;
        log.append(&sample("01K5CQXM8N7VZR3TFWJ0HB2YQE", None))?;

        let contents: String = std::fs::read_to_string(&log.path)?;
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);

        let first: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(first["kind"], "delegate");
        assert_eq!(first["session_id"], "01K5CQXM8N7VZR3TFWJ0HB2YQD");
        assert_eq!(first["mode"], "inspect");
        assert_eq!(first["status"], "completed");
        assert_eq!(first["duration_ms"], 12_500);
        assert_eq!(first["usage"]["cached_input_tokens"], 64);
        assert_eq!(first["worker_startup_ms"], 340);

        let second: serde_json::Value = serde_json::from_str(lines[1])?;
        assert!(second.get("usage").is_none());

        std::fs::remove_dir_all(&root)?;
        Ok(())
    }
}
