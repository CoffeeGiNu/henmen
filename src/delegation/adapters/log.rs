use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use crate::delegation::{application::RunRecord, ports::RunLog};

pub struct RunLogFile {
    path: PathBuf,
}

impl RunLogFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl RunLog for RunLogFile {
    fn append(&self, record: &RunRecord) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut line: Vec<u8> = serde_json::to_vec(record)?;
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
    use crate::delegation::application::{
        Mode, RunKind, RunRecord, SessionStatus, TokenUsage, WorkerStatus,
    };

    fn sample(
        session_id: &str,
        usage: Option<TokenUsage>,
        worker_status: Option<WorkerStatus>,
    ) -> RunRecord {
        RunRecord {
            timestamp_start: 1_757_894_400_000,
            timestamp_end: 1_757_894_412_500,
            duration_milliseconds: 12_500,
            kind: RunKind::Delegate,
            session_id: session_id.parse().expect("valid session id"),
            model: "gpt-5.6-luna".to_string(),
            effort: "xhigh".to_string(),
            mode: Mode::Inspect,
            session_status: SessionStatus::Completed,
            worker_status,
            usage,
            worker_startup_milliseconds: Some(340),
        }
    }

    #[test]
    fn appends_one_line_per_record() -> anyhow::Result<()> {
        let root: std::path::PathBuf =
            std::env::temp_dir().join(format!("henmen-run-log-test-{}", std::process::id()));
        let run_log: RunLogFile = RunLogFile::new(root.join("logs").join("runs.jsonl"));

        run_log.append(&sample(
            "01K5CQXM8N7VZR3TFWJ0HB2YQD",
            Some(TokenUsage {
                input_tokens: Some(120),
                cached_input_tokens: Some(64),
                output_tokens: Some(30),
            }),
            Some(WorkerStatus::Done),
        ))?;
        run_log.append(&sample("01K5CQXM8N7VZR3TFWJ0HB2YQE", None, None))?;

        let contents: String = std::fs::read_to_string(&run_log.path)?;
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);

        let first: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(first["kind"], "delegate");
        assert_eq!(first["session_id"], "01K5CQXM8N7VZR3TFWJ0HB2YQD");
        assert_eq!(first["mode"], "inspect");
        assert_eq!(first["session_status"], "completed");
        assert_eq!(first["worker_status"], "done");
        assert_eq!(first["duration_ms"], 12_500);
        assert_eq!(first["usage"]["cached_input_tokens"], 64);
        assert_eq!(first["worker_startup_ms"], 340);

        let second: serde_json::Value = serde_json::from_str(lines[1])?;
        assert!(second.get("usage").is_none());
        assert_eq!(second.get("worker_status"), Some(&serde_json::Value::Null));

        std::fs::remove_dir_all(&root)?;
        Ok(())
    }
}
