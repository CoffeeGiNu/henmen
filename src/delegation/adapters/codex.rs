use serde::Deserialize;
use serde_json::Value;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::time::{Duration, Instant};

use crate::delegation::application::{
    Interrupted, Mode, RateLimitObservation, RateLimitWindow, ThreadId, TimedOut, TokenUsage,
    WorkerMetrics, WorkerRequest, WorkerResponse,
};
use crate::delegation::ports::{ModelCatalog, Worker, WorkerThread};
use crate::termination::Termination;

pub struct CodexProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    incoming: Receiver<String>,
    next_request_id: u64,
    pending_notifications: VecDeque<serde_json::Value>,
    startup_milliseconds: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Deadline {
    instant: Instant,
    timeout: Duration,
}

impl Deadline {
    fn after(timeout: Duration) -> Self {
        Self {
            instant: Instant::now() + timeout,
            timeout,
        }
    }

    fn remaining(&self) -> Option<Duration> {
        self.instant.checked_duration_since(Instant::now())
    }
}

impl CodexProcess {
    pub fn spawn() -> anyhow::Result<Self> {
        let started_at: Instant = Instant::now();
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
        let (sender, receiver): (Sender<String>, Receiver<String>) = channel();
        std::thread::spawn(move || {
            let reader: BufReader<ChildStdout> = BufReader::new(stdout);
            for line_result in reader.lines() {
                let line: String = match line_result {
                    Ok(line) => line,
                    Err(_) => break,
                };
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let mut process: Self = Self {
            child,
            stdin: Some(stdin),
            incoming: receiver,
            next_request_id: 0,
            pending_notifications: VecDeque::new(),
            startup_milliseconds: 0,
        };
        process.request(
            "initialize",
            serde_json::json!({
                "clientInfo": {
                    "name":"henmen", "version": env!("CARGO_PKG_VERSION")
                }
            }),
            None,
        )?;
        process.notify("initialized", serde_json::json!({}))?;
        process.startup_milliseconds = started_at.elapsed().as_millis() as u64;
        Ok(process)
    }

    pub fn request(
        &mut self,
        method: &str,
        params: serde_json::Value,
        deadline: Option<Deadline>,
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
            let mut incoming_message: serde_json::Value =
                receive_message(&self.incoming, deadline)?;
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

    pub fn wait_for_notification(
        &mut self,
        method: &str,
        deadline: Option<Deadline>,
    ) -> anyhow::Result<serde_json::Value> {
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
            let incoming_message: serde_json::Value = receive_message(&self.incoming, deadline)?;
            if incoming_message["method"] == method {
                return Ok(incoming_message);
            }
            self.pending_notifications.push_back(incoming_message);
        }
    }

    fn read_rate_limit_observation(&mut self) -> Option<RateLimitObservation> {
        self.request(
            "account/rateLimits/read",
            serde_json::json!({}),
            Some(Deadline::after(Duration::from_secs(10))),
        )
        .ok()
        .and_then(|result: serde_json::Value| parse_rate_limit_observation(&result))
    }

    pub fn child_process_id(&self) -> u32 {
        self.child.id()
    }

    pub fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    pub fn shutdown(&mut self) -> anyhow::Result<()> {
        self.stdin.take();
        let deadline: Deadline = Deadline::after(Duration::from_secs(10));
        loop {
            match self.child.try_wait()? {
                Some(_) => return Ok(()),
                None => {
                    if deadline.remaining().is_none() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
        self.child.kill()?;
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

    fn shutdown_after_error(
        &self,
        mut process: CodexProcess,
        error: anyhow::Error,
    ) -> anyhow::Error {
        let shutdown_result: anyhow::Result<()> = process.shutdown();
        self.termination.clear_child_process_id();
        match shutdown_result {
            Ok(()) => error,
            Err(shutdown_error) => {
                let combined: String = format!(
                    "{error:#} (additionally, the worker shutdown failed: {shutdown_error:#})"
                );
                error.context(combined)
            }
        }
    }
}

// TODO: need nextCursor for model/list command for the future?
impl ModelCatalog for CodexWorker {
    fn list_models(&self) -> anyhow::Result<serde_json::Value> {
        let mut process: CodexProcess = self.spawn_registered()?;
        let response: serde_json::Value = match process.request(
            "model/list",
            serde_json::json!({}),
            Some(Deadline::after(Duration::from_secs(10))),
        ) {
            Ok(response) => response,
            Err(error) => return Err(self.shutdown_after_error(process, error)),
        };

        let shutdown: anyhow::Result<()> = process.shutdown();
        self.termination.clear_child_process_id();
        shutdown?;
        Ok(response)
    }
}

impl Worker for CodexWorker {
    fn start(&self, request: &WorkerRequest) -> anyhow::Result<Box<dyn WorkerThread>> {
        let mut process: CodexProcess = self.spawn_registered()?;
        let result: serde_json::Value = match process.request(
            "thread/start",
            serde_json::json!({
                "config": { "features": { "memories": false, "plugins": false } },
                "cwd": request.cwd,
                "sandbox": to_codex_sandbox_mode(request.mode),
                "approvalPolicy": "never",
            }),
            None,
        ) {
            Ok(result) => result,
            Err(error) => return Err(self.shutdown_after_error(process, error)),
        };
        // NOTE: henmen's session.session_id != thread.sessionId.
        let thread_id: ThreadId = match result["thread"]["id"].as_str() {
            Some(value) => ThreadId(value.to_string()),
            None => {
                return Err(self.shutdown_after_error(
                    process,
                    anyhow::anyhow!("thread/start returned no thread id"),
                ));
            }
        };
        Ok(Box::new(CodexThread {
            process,
            thread_id,
            active_turn_id: None,
            usage: None,
            rate_limit_before: None,
            rate_limit_after: None,
            termination: Arc::clone(&self.termination),
        }))
    }

    fn resume(
        &self,
        thread_id: &ThreadId,
        request: &WorkerRequest,
    ) -> anyhow::Result<Box<dyn WorkerThread>> {
        let mut process: CodexProcess = self.spawn_registered()?;
        match process.request(
            "thread/resume",
            serde_json::json!({
                "config": { "features": { "memories": false, "plugins": false } },
                "threadId": thread_id.0,
                "cwd": request.cwd,
                "sandbox": to_codex_sandbox_mode(request.mode),
                "approvalPolicy": "never",
            }),
            None,
        ) {
            Ok(_) => {}
            Err(error) => return Err(self.shutdown_after_error(process, error)),
        }
        Ok(Box::new(CodexThread {
            process,
            thread_id: thread_id.clone(),
            active_turn_id: None,
            usage: None,
            rate_limit_before: None,
            rate_limit_after: None,
            termination: Arc::clone(&self.termination),
        }))
    }
}

pub struct CodexThread {
    process: CodexProcess,
    thread_id: ThreadId,
    active_turn_id: Option<String>,
    usage: Option<TokenUsage>,
    rate_limit_before: Option<RateLimitObservation>,
    rate_limit_after: Option<RateLimitObservation>,
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

    fn metrics(&self) -> WorkerMetrics {
        WorkerMetrics {
            usage: self.usage,
            startup_milliseconds: Some(self.process.startup_milliseconds),
            rate_limit_before: self.rate_limit_before.clone(),
            rate_limit_after: self.rate_limit_after.clone(),
        }
    }

    fn shutdown(&mut self) -> anyhow::Result<()> {
        let active_turn_id: Option<String> = self.active_turn_id.take();
        let interrupt_result: anyhow::Result<()> = match active_turn_id {
            Some(turn_id) if self.process.is_running() => self
                .process
                .request(
                    "turn/interrupt",
                    serde_json::json!({ "threadId": self.thread_id.0, "turnId": turn_id }),
                    Some(Deadline::after(Duration::from_secs(10))),
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
        let deadline: Deadline = Deadline::after(request.timeout);
        self.rate_limit_before = self.process.read_rate_limit_observation();
        let result: serde_json::Value = self.process.request(
            "turn/start",
            serde_json::json!({
                "threadId": self.thread_id.0,
                "input": [{ "type": "text", "text": request.task }],
                "model": request.model,
                "effort": request.effort,
                "outputSchema": output_schema(),
            }),
            Some(deadline),
        )?;
        self.active_turn_id = result["turn"]["id"].as_str().map(str::to_string);

        let mut notification: serde_json::Value = self
            .process
            .wait_for_notification("turn/completed", Some(deadline))?;
        self.rate_limit_after = self.process.read_rate_limit_observation();
        self.active_turn_id = None;

        let turn: Turn = serde_json::from_value(notification["params"]["turn"].take())?;
        self.usage = token_usage(&self.process.pending_notifications, &turn.id);

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

fn receive_message(
    incoming: &Receiver<String>,
    deadline: Option<Deadline>,
) -> anyhow::Result<serde_json::Value> {
    let line: String = match deadline {
        None => incoming
            .recv()
            .map_err(|_| anyhow::anyhow!("end of stream"))?,
        Some(deadline) => {
            let remaining: Duration = deadline.remaining().ok_or_else(|| {
                anyhow::Error::new(TimedOut {
                    timeout: deadline.timeout,
                })
            })?;
            incoming
                .recv_timeout(remaining)
                .map_err(|error: RecvTimeoutError| match error {
                    RecvTimeoutError::Timeout => anyhow::Error::new(TimedOut {
                        timeout: deadline.timeout,
                    }),
                    RecvTimeoutError::Disconnected => anyhow::anyhow!("end of stream"),
                })?
        }
    };
    Ok(serde_json::from_str(&line)?)
}

fn parse_rate_limit_observation(result: &serde_json::Value) -> Option<RateLimitObservation> {
    let rate_limits: &serde_json::Map<String, serde_json::Value> =
        result.get("rateLimits")?.as_object()?;
    let limit_id: Option<String> = rate_limits
        .get("limitId")
        .and_then(Value::as_str)
        .map(str::to_string);
    let primary: Option<RateLimitWindow> =
        rate_limits.get("primary").and_then(parse_rate_limit_window);
    let secondary: Option<RateLimitWindow> = rate_limits
        .get("secondary")
        .and_then(parse_rate_limit_window);
    Some(RateLimitObservation {
        limit_id,
        primary,
        secondary,
    })
}

fn parse_rate_limit_window(value: &serde_json::Value) -> Option<RateLimitWindow> {
    let window: &serde_json::Map<String, serde_json::Value> = value.as_object()?;
    let used_percent: f64 = window.get("usedPercent")?.as_f64()?;
    let window_duration_minutes: Option<u64> =
        window.get("windowDurationMins").and_then(Value::as_u64);
    let resets_at: Option<i64> = window.get("resetsAt").and_then(Value::as_i64);
    Some(RateLimitWindow {
        used_percent,
        window_duration_minutes,
        resets_at,
    })
}

fn token_delta(first: &serde_json::Value, last: &serde_json::Value, field: &str) -> Option<u64> {
    let first_total: u64 = first["tokenUsage"]["total"][field].as_u64()?;
    let first_last: u64 = first["tokenUsage"]["last"][field].as_u64()?;
    let last_total: u64 = last["tokenUsage"]["total"][field].as_u64()?;
    let baseline: u64 = first_total.checked_sub(first_last)?;
    last_total.checked_sub(baseline)
}

fn token_usage(notifications: &VecDeque<serde_json::Value>, turn_id: &str) -> Option<TokenUsage> {
    let updates: Vec<&serde_json::Value> = notifications
        .iter()
        .filter(|notification| {
            notification["method"] == "thread/tokenUsage/updated"
                && notification["params"]["turnId"] == turn_id
        })
        .map(|notification| &notification["params"])
        .collect();
    let first: &serde_json::Value = updates.first().copied()?;
    let last: &serde_json::Value = updates.last().copied().unwrap_or(first);
    Some(TokenUsage {
        input_tokens: token_delta(first, last, "inputTokens"),
        cached_input_tokens: token_delta(first, last, "cachedInputTokens"),
        output_tokens: token_delta(first, last, "outputTokens"),
    })
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
    fn parses_rate_limit_observation_with_both_windows() {
        let result: serde_json::Value = serde_json::json!({
            "rateLimits": {
                "limitId": "codex",
                "limitName": "Codex account",
                "primary": {
                    "usedPercent": 12,
                    "windowDurationMins": 300,
                    "resetsAt": 1789519477
                },
                "secondary": {
                    "usedPercent": 40,
                    "windowDurationMins": 10080,
                    "resetsAt": 1789519477
                }
            },
            "rateLimitsByLimitId": {
                "codex": {
                    "ignored": true
                }
            },
            "credits": {
                "ignored": true
            }
        });
        let observation: Option<RateLimitObservation> = parse_rate_limit_observation(&result);

        assert_eq!(
            observation,
            Some(RateLimitObservation {
                limit_id: Some("codex".to_string()),
                primary: Some(RateLimitWindow {
                    used_percent: 12.0,
                    window_duration_minutes: Some(300),
                    resets_at: Some(1789519477),
                }),
                secondary: Some(RateLimitWindow {
                    used_percent: 40.0,
                    window_duration_minutes: Some(10080),
                    resets_at: Some(1789519477),
                }),
            })
        );
    }

    #[test]
    fn returns_none_without_rate_limits() {
        let result: serde_json::Value = serde_json::json!({
            "rateLimitsByLimitId": {},
            "credits": {}
        });

        assert_eq!(parse_rate_limit_observation(&result), None);
    }

    fn token_usage_update(
        turn_id: &str,
        last: (u64, u64, u64),
        total: (u64, u64, u64),
    ) -> serde_json::Value {
        serde_json::json!({
            "method": "thread/tokenUsage/updated",
            "params": {
                "turnId": turn_id,
                "tokenUsage": {
                    "last": {
                        "inputTokens": last.0,
                        "cachedInputTokens": last.1,
                        "outputTokens": last.2
                    },
                    "total": {
                        "inputTokens": total.0,
                        "cachedInputTokens": total.1,
                        "outputTokens": total.2
                    }
                }
            }
        })
    }

    #[test]
    fn counts_a_turn_as_the_growth_of_the_running_total() {
        let notifications: VecDeque<serde_json::Value> = VecDeque::from(vec![
            serde_json::json!({"method": "turn/started"}),
            token_usage_update("turn_1", (100, 0, 20), (100, 0, 20)),
            token_usage_update("turn_1", (150, 64, 30), (250, 64, 50)),
        ]);

        assert_eq!(
            token_usage(&notifications, "turn_1"),
            Some(TokenUsage {
                input_tokens: Some(250),
                cached_input_tokens: Some(64),
                output_tokens: Some(50),
            })
        );
    }

    #[test]
    fn leaves_out_the_tokens_a_resumed_thread_spent_earlier() {
        let notifications: VecDeque<serde_json::Value> = VecDeque::from(vec![
            token_usage_update("turn_1", (12_848, 6_000, 90), (12_848, 6_000, 90)),
            token_usage_update("turn_2", (12_776, 12_672, 40), (25_624, 18_672, 130)),
        ]);

        assert_eq!(
            token_usage(&notifications, "turn_2"),
            Some(TokenUsage {
                input_tokens: Some(12_776),
                cached_input_tokens: Some(12_672),
                output_tokens: Some(40),
            })
        );
    }

    #[test]
    fn ignores_a_re_emitted_update_that_did_not_move_the_total() {
        let notifications: VecDeque<serde_json::Value> = VecDeque::from(vec![
            token_usage_update("turn_1", (100, 0, 20), (100, 0, 20)),
            token_usage_update("turn_1", (100, 0, 20), (100, 0, 20)),
        ]);

        assert_eq!(
            token_usage(&notifications, "turn_1"),
            Some(TokenUsage {
                input_tokens: Some(100),
                cached_input_tokens: Some(0),
                output_tokens: Some(20),
            })
        );
    }

    #[test]
    fn reports_nothing_when_the_running_total_went_backwards() {
        let notifications: VecDeque<serde_json::Value> = VecDeque::from(vec![
            token_usage_update("turn_1", (100, 64, 20), (1_100, 1_064, 120)),
            token_usage_update("turn_1", (150, 64, 30), (900, 900, 90)),
        ]);

        assert_eq!(
            token_usage(&notifications, "turn_1"),
            Some(TokenUsage {
                input_tokens: None,
                cached_input_tokens: None,
                output_tokens: None,
            })
        );
    }

    #[test]
    fn reports_no_usage_without_a_notification() {
        let notifications: VecDeque<serde_json::Value> =
            VecDeque::from(vec![serde_json::json!({"method": "turn/completed"})]);

        assert_eq!(token_usage(&notifications, "turn_1"), None);
    }

    #[test]
    fn receives_two_frames_in_order() -> anyhow::Result<()> {
        let (sender, incoming): (Sender<String>, Receiver<String>) = channel();
        sender.send(r#"{"id":1,"result":{}}"#.to_string())?;
        sender.send(r#"{"method":"turn/completed"}"#.to_string())?;

        let first_message: serde_json::Value = receive_message(&incoming, None)?;
        let second_message: serde_json::Value = receive_message(&incoming, None)?;

        assert_eq!(first_message["id"], 1);
        assert_eq!(second_message["method"], "turn/completed");
        Ok(())
    }

    #[test]
    fn times_out_when_no_frame_arrives() {
        let (_sender, incoming): (Sender<String>, Receiver<String>) = channel();
        let deadline: Deadline = Deadline::after(Duration::from_millis(50));

        let error: anyhow::Error = receive_message(&incoming, Some(deadline))
            .expect_err("receive_message should time out");

        assert!(error.downcast_ref::<TimedOut>().is_some());
    }

    #[test]
    fn rejects_a_dropped_sender() {
        let (sender, incoming): (Sender<String>, Receiver<String>) = channel();
        drop(sender);

        let error: anyhow::Error =
            receive_message(&incoming, None).expect_err("receive_message should reject EOF");

        assert_eq!(error.to_string(), "end of stream");
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
            timeout: Duration::from_secs(30 * 60),
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

        let result: serde_json::Value =
            process.request("model/list", serde_json::json!({}), None)?;

        assert!(result.is_object());

        process.shutdown()?;
        Ok(())
    }
}
