use anyhow::Context;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Instant, SystemTime};

use crate::delegation::ports::{LogSink, SessionStore, Worker, WorkerThread};

#[derive(Debug, thiserror::Error)]
#[error("the worker was interrupted")]
pub struct Interrupted;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub String);

impl SessionId {
    pub fn new() -> Self {
        Self(ulid::Ulid::generate().to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Inspect,
    Edit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionStatus {
    Running,
    Completed,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub session_id: SessionId,
    pub thread_id: ThreadId,
    pub cwd: PathBuf,
    pub model: String,
    pub effort: String,
    pub mode: Mode,
    pub status: SessionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ThreadId(pub String);

#[derive(Debug, Clone)]
pub struct WorkerRequest {
    pub task: String,
    pub cwd: PathBuf,
    pub model: String,
    pub effort: String,
    pub mode: Mode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WorkerStatus {
    Done,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerResponse {
    pub status: WorkerStatus,
    pub summary: String,
    pub evidence: Vec<String>,
    pub changed_files: Vec<PathBuf>,
    pub tests: Vec<String>,
    pub open_questions: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Delegate,
    Resume,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkerMetrics {
    pub usage: Option<Usage>,
    pub startup_milliseconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp_start: u64,
    pub timestamp_end: u64,
    #[serde(rename = "duration_ms")]
    pub duration_milliseconds: u64,
    pub kind: Operation,
    pub session_id: SessionId,
    pub model: String,
    pub effort: String,
    pub mode: Mode,
    pub status: SessionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(rename = "worker_startup_ms", skip_serializing_if = "Option::is_none")]
    pub worker_startup_milliseconds: Option<u64>,
}

fn epoch_milliseconds(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn write_log_entry(
    log: Option<&dyn LogSink>,
    kind: Operation,
    session: &Session,
    metrics: &WorkerMetrics,
    start: (SystemTime, Instant),
) {
    let Some(log) = log else {
        return;
    };
    let entry: LogEntry = LogEntry {
        timestamp_start: epoch_milliseconds(start.0),
        timestamp_end: epoch_milliseconds(SystemTime::now()),
        duration_milliseconds: start.1.elapsed().as_millis() as u64,
        kind,
        session_id: session.session_id.clone(),
        model: session.model.clone(),
        effort: session.effort.clone(),
        mode: session.mode,
        status: session.status,
        usage: metrics.usage,
        worker_startup_milliseconds: metrics.startup_milliseconds,
    };
    if let Err(error) = log.append(&entry) {
        eprintln!("henmen: could not write the log entry: {error}");
    }
}

pub fn delegate(
    worker: &dyn Worker,
    store: &dyn SessionStore,
    log: Option<&dyn LogSink>,
    request: &WorkerRequest,
) -> anyhow::Result<(SessionId, WorkerResponse)> {
    let start: (SystemTime, Instant) = (SystemTime::now(), Instant::now());
    let mut thread: Box<dyn WorkerThread> = worker.start(request)?;
    let mut session: Session = Session {
        session_id: SessionId::new(),
        thread_id: thread.thread_id().clone(),
        cwd: request.cwd.clone(),
        model: request.model.clone(),
        effort: request.effort.clone(),
        mode: request.mode,
        status: SessionStatus::Running,
    };
    store.save(&session)?;

    let result: anyhow::Result<WorkerResponse> = thread.turn(request);
    session.status = match &result {
        Ok(_) => SessionStatus::Completed,
        Err(error) if error.downcast_ref::<Interrupted>().is_some() => SessionStatus::Interrupted,
        Err(_) => SessionStatus::Failed,
    };
    store.save(&session)?;
    write_log_entry(log, Operation::Delegate, &session, &thread.metrics(), start);

    let shutdown_result: anyhow::Result<()> = thread.shutdown();
    let response: WorkerResponse =
        result.with_context(|| format!("session: {} failed", session.session_id.0))?;
    shutdown_result?;
    Ok((session.session_id, response))
}

pub fn resume(
    worker: &dyn Worker,
    store: &dyn SessionStore,
    log: Option<&dyn LogSink>,
    session_id: &SessionId,
    request: &WorkerRequest,
) -> anyhow::Result<(SessionId, WorkerResponse)> {
    let start: (SystemTime, Instant) = (SystemTime::now(), Instant::now());
    let mut session: Session = store.load(session_id)?;
    let mut thread: Box<dyn WorkerThread> = worker.resume(&session.thread_id, request)?;
    // TODO: to be method at session?
    session.cwd = request.cwd.clone();
    session.model = request.model.clone();
    session.effort = request.effort.clone();
    session.mode = request.mode;
    session.status = SessionStatus::Running;
    store.save(&session)?;

    let result: anyhow::Result<WorkerResponse> = thread.turn(request);
    session.status = match &result {
        Ok(_) => SessionStatus::Completed,
        Err(error) if error.downcast_ref::<Interrupted>().is_some() => SessionStatus::Interrupted,
        Err(_) => SessionStatus::Failed,
    };
    store.save(&session)?;
    write_log_entry(log, Operation::Resume, &session, &thread.metrics(), start);

    let shutdown_result: anyhow::Result<()> = thread.shutdown();
    let response: WorkerResponse =
        result.with_context(|| format!("session: {} failed", session.session_id.0))?;
    shutdown_result?;
    Ok((session.session_id, response))
}
