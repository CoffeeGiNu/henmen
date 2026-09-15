use serde::Deserialize;
use serde_json::Value;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use crate::delegation::application::{Interrupted, Mode, ThreadId, WorkerRequest, WorkerResponse};
use crate::delegation::ports::{Worker, WorkerThread};
use crate::termination::Termination;
use std::sync::Arc;

pub struct CodexProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_request_id: u64,
    pending_notifications: VecDeque<serde_json::Value>,
}

impl CodexProcess {
    pub fn spawn() -> anyhow::Result<Self> {
        let mut child: Child = Command::new("codex")
            .arg("app-server")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let stdin: ChildStdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("codex app server did not expose stdin"))?;
        let stdout: ChildStdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("codex app server did not expose stdout"))?;
        let mut process: Self = Self {
            child,
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
            next_request_id: 0,
            pending_notifications: VecDeque::new(),
        };
        process.request(
            "initialize",
            serde_json::json!({
                "clientInfo": {
                    "name":"henmen", "version": env!("CARGO_PKG_VERSION")
                }
            }),
        )?;
        process.notify("initialized", serde_json::json!({}))?;
        Ok(process)
    }

    pub fn request(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let request_id: u64 = self.next_request_id;
        self.next_request_id += 1;
        let request_message: serde_json::Value = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        });
        let stdin: &mut ChildStdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("codex app server stdin is already closed"))?;
        writeln!(stdin, "{request_message}")?;
        stdin.flush()?;

        loop {
            let mut incoming_message: serde_json::Value = read_message(&mut self.stdout)?;
            if incoming_message["id"] != request_id {
                self.pending_notifications.push_back(incoming_message);
                continue;
            }
            if let Some(error) = incoming_message.get("error") {
                anyhow::bail!("codex app server rejected {method}: {error}");
            }
            return Ok(incoming_message["result"].take());
        }
    }

    pub fn notify(&mut self, method: &str, params: serde_json::Value) -> anyhow::Result<()> {
        let notification_message: serde_json::Value = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let stdin: &mut ChildStdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("codex app server stdin is already closed"))?;
        writeln!(stdin, "{notification_message}")?;
        stdin.flush()?;
        Ok(())
    }

    pub fn wait_for_notification(&mut self, method: &str) -> anyhow::Result<serde_json::Value> {
        let position: Option<usize> = self
            .pending_notifications
            .iter()
            .position(|notification| notification["method"] == method);
        if let Some(index) = position {
            let notification: serde_json::Value = self
                .pending_notifications
                .remove(index)
                .expect("position() returned an index that is in range");
            return Ok(notification);
        }

        loop {
            let incoming_message: serde_json::Value = read_message(&mut self.stdout)?;
            if incoming_message["method"] == method {
                return Ok(incoming_message);
            }
            self.pending_notifications.push_back(incoming_message);
        }
    }

    pub fn child_process_id(&self) -> u32 {
        self.child.id()
    }

    pub fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    pub fn shutdown(&mut self) -> anyhow::Result<()> {
        self.stdin.take();
        self.child.wait()?;
        Ok(())
    }
}

impl Drop for CodexProcess {
    fn drop(&mut self) {
        self.stdin.take();
    }
}

pub struct CodexWorker {
    termination: Arc<Termination>,
}

impl CodexWorker {
    pub fn new(termination: Arc<Termination>) -> Self {
        Self { termination }
    }

    fn spawn_registered(&self) -> anyhow::Result<CodexProcess> {
        let process: CodexProcess = CodexProcess::spawn()?;
        self.termination
            .set_child_process_id(process.child_process_id());
        Ok(process)
    }
}

impl Worker for CodexWorker {
    fn start(&self, request: &WorkerRequest) -> anyhow::Result<Box<dyn WorkerThread>> {
        let mut process: CodexProcess = self.spawn_registered()?;
        let result: serde_json::Value = process.request(
            "thread/start",
            serde_json::json!({
                "config": { "features": { "memories": false, "plugins": false } },
                "cwd": request.cwd,
                "sandbox": to_codex_sandbox_mode(request.mode),
                "approvalPolicy": "never",
            }),
        )?;
        // NOTE: henmen's session.session_id != thread.sessionId.
        let thread_id: ThreadId = ThreadId(
            result["thread"]["id"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("thread/start returned no thread id"))?
                .to_string(),
        );
        Ok(Box::new(CodexThread {
            process,
            thread_id,
            active_turn_id: None,
            termination: Arc::clone(&self.termination),
        }))
    }

    fn resume(
        &self,
        thread_id: &ThreadId,
        request: &WorkerRequest,
    ) -> anyhow::Result<Box<dyn WorkerThread>> {
        let mut process: CodexProcess = self.spawn_registered()?;
        process.request(
            "thread/resume",
            serde_json::json!({
                "config": { "features": { "memories": false, "plugins": false } },
                "threadId": thread_id.0,
                "cwd": request.cwd,
                "sandbox": to_codex_sandbox_mode(request.mode),
                "approvalPolicy": "never",
            }),
        )?;
        Ok(Box::new(CodexThread {
            process,
            thread_id: thread_id.clone(),
            active_turn_id: None,
            termination: Arc::clone(&self.termination),
        }))
    }
}

pub struct CodexThread {
    process: CodexProcess,
    thread_id: ThreadId,
    active_turn_id: Option<String>,
    termination: Arc<Termination>,
}

impl WorkerThread for CodexThread {
    fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    fn turn(&mut self, request: &WorkerRequest) -> anyhow::Result<WorkerResponse> {
        self.turn_inner(request).map_err(|error| {
            if self.termination.is_requested() {
                error.context(Interrupted)
            } else {
                error
            }
        })
    }

    fn shutdown(&mut self) -> anyhow::Result<()> {
        let active_turn_id: Option<String> = self.active_turn_id.take();
        let interrupt_result: anyhow::Result<()> = match active_turn_id {
            Some(turn_id) if self.process.is_running() => self
                .process
                .request(
                    "turn/interrupt",
                    serde_json::json!({ "threadId": self.thread_id.0, "turnId": turn_id }),
                )
                .map(|_| ()),
            _ => Ok(()),
        };
        let shutdown_result: anyhow::Result<()> = self.process.shutdown();
        self.termination.clear_child_process_id();
        shutdown_result.and(interrupt_result)
    }
}

impl CodexThread {
    fn turn_inner(&mut self, request: &WorkerRequest) -> anyhow::Result<WorkerResponse> {
        let result: serde_json::Value = self.process.request(
            "turn/start",
            serde_json::json!({
                "threadId": self.thread_id.0,
                "input": [{ "type": "text", "text": request.task }],
                "model": request.model,
                "effort": request.effort,
                "outputSchema": output_schema(),
            }),
        )?;
        self.active_turn_id = result["turn"]["id"].as_str().map(str::to_string);

        let mut notification: serde_json::Value =
            self.process.wait_for_notification("turn/completed")?;
        self.active_turn_id = None;

        let turn: Turn = serde_json::from_value(notification["params"]["turn"].take())?;
        match turn.status {
            TurnStatus::Completed => parse_worker_response(&turn),
            TurnStatus::Failed => anyhow::bail!(
                "turn {} failed: {}",
                turn.id,
                turn.error.unwrap_or(Value::Null)
            ),
            TurnStatus::Interrupted => anyhow::bail!("turn {} was interrupted", turn.id),
            TurnStatus::InProgress => {
                anyhow::bail!("turn {} is still in progress after turn/completed", turn.id)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct Turn {
    id: String,
    items: Vec<TurnItem>,
    status: TurnStatus,
    error: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum TurnStatus {
    Completed,
    Interrupted,
    Failed,
    InProgress,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum TurnItem {
    #[serde(rename = "agentMessage")]
    AgentMessage {
        #[allow(dead_code)]
        id: String,
        text: String,
    },

    #[serde(other)]
    Other,
}

fn read_message(reader: &mut impl BufRead) -> anyhow::Result<serde_json::Value> {
    let mut line: String = String::new();
    let read_bytes: usize = reader.read_line(&mut line)?;
    if read_bytes == 0 {
        anyhow::bail!("end of stream");
    }
    Ok(serde_json::from_str(&line)?)
}

fn to_codex_sandbox_mode(mode: Mode) -> &'static str {
    match mode {
        Mode::Inspect => "read-only",
        Mode::Edit => "workspace-write",
    }
}

fn output_schema() -> serde_json::Value {
    schemars::schema_for!(WorkerResponse).to_value()
}

fn parse_worker_response(turn: &Turn) -> anyhow::Result<WorkerResponse> {
    let text: &String = turn
        .items
        .iter()
        .rev()
        .find_map(|item| match item {
            TurnItem::AgentMessage { text, .. } => Some(text),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("turn contains no agent message"))?;
    Ok(serde_json::from_str(text)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delegation::application::WorkerStatus;
    use std::path::PathBuf;

    #[test]
    fn maps_mode_to_sandbox() {
        assert_eq!(to_codex_sandbox_mode(Mode::Inspect), "read-only");
        assert_eq!(to_codex_sandbox_mode(Mode::Edit), "workspace-write");
    }

    #[test]
    fn output_schema_requires_every_contract_field() {
        let schema: serde_json::Value = output_schema();
        let required: &Vec<serde_json::Value> =
            schema["required"].as_array().expect("required is an array");

        for field in [
            "status",
            "summary",
            "evidence",
            "changed_files",
            "tests",
            "open_questions",
        ] {
            assert!(
                required.iter().any(|value| value.as_str() == Some(field)),
                "output_schema is missing required field {field}"
            );
        }
    }

    #[test]
    fn reads_one_frame_per_line() -> anyhow::Result<()> {
        let mut input: &[u8] = b"{\"id\":1,\"result\":{}}\n{\"method\":\"turn/completed\"}\n";

        assert_eq!(read_message(&mut input)?["id"], 1);
        assert_eq!(read_message(&mut input)?["method"], "turn/completed");
        Ok(())
    }

    #[test]
    fn rejects_end_of_stream() {
        let mut input: &[u8] = b"";

        assert!(read_message(&mut input).is_err());
    }

    #[test]
    fn parses_worker_response_from_last_agent_message() -> anyhow::Result<()> {
        let turn: Turn = serde_json::from_value(serde_json::json!({
            "id": "turn_1",
            "status": "completed",
            "items": [
                {"type": "commandExecution", "id": "item_1"},
                {"type": "agentMessage", "id": "item_2", "text": "let me take a look"},
                {"type": "agentMessage", "id": "item_3", "text": r#"{"status":"done","summary":"fixed the retry branch","evidence":["src/retry.rs:120-145"],"changed_files":["src/retry.rs"],"tests":["cargo test: 12 passed"],"open_questions":[]}"#}
            ]
        }))?;

        let response: WorkerResponse = parse_worker_response(&turn)?;

        assert_eq!(response.status, WorkerStatus::Done);
        assert_eq!(response.summary, "fixed the retry branch");
        assert_eq!(response.evidence, vec!["src/retry.rs:120-145".to_string()]);
        assert_eq!(response.changed_files, vec![PathBuf::from("src/retry.rs")]);
        assert_eq!(response.tests, vec!["cargo test: 12 passed".to_string()]);
        assert!(response.open_questions.is_empty());
        Ok(())
    }

    #[test]
    fn detects_blocked_status() -> anyhow::Result<()> {
        let turn: Turn = serde_json::from_value(serde_json::json!({
            "id": "turn_1",
            "status": "completed",
            "items": [
                {"type": "agentMessage", "id": "item_1", "text": r#"{"status":"blocked","summary":"could not create the file because the workspace is read-only","evidence":[],"changed_files":[],"tests":[],"open_questions":["should this be re-run in edit mode?"]}"#}
            ]
        }))?;

        let response: WorkerResponse = parse_worker_response(&turn)?;

        assert_eq!(response.status, WorkerStatus::Blocked);
        Ok(())
    }

    #[test]
    fn rejects_turn_without_agent_message() -> anyhow::Result<()> {
        let turn: Turn = serde_json::from_value(serde_json::json!({
            "id": "turn_1",
            "status": "completed",
            "items": [{"type": "commandExecution", "id": "item_1"}]
        }))?;

        assert!(parse_worker_response(&turn).is_err());
        Ok(())
    }

    #[test]
    #[ignore = "requires a live codex app-server process"]
    fn runs_one_turn_against_real_app_server() -> anyhow::Result<()> {
        let worker: CodexWorker = CodexWorker::new(Arc::new(Termination::new()));
        let request: WorkerRequest = WorkerRequest {
            task: "Answer from your own knowledge without using any tools: what is 2 + 2?"
                .to_string(),
            cwd: std::env::current_dir()?,
            model: "gpt-5.6-luna".to_string(),
            effort: "low".to_string(),
            mode: Mode::Inspect,
        };

        let mut thread: Box<dyn WorkerThread> = worker.start(&request)?;
        assert!(!thread.thread_id().0.is_empty());

        let response: WorkerResponse = thread.turn(&request)?;
        assert_eq!(response.status, WorkerStatus::Done);
        assert!(!response.summary.is_empty());

        thread.shutdown()?;
        Ok(())
    }

    #[test]
    #[ignore = "requires a live codex app-server process"]
    fn completes_handshake_against_real_app_server() -> anyhow::Result<()> {
        let mut process: CodexProcess = CodexProcess::spawn()?;

        let result: serde_json::Value = process.request("model/list", serde_json::json!({}))?;

        assert!(result.is_object());

        process.shutdown()?;
        Ok(())
    }
}
