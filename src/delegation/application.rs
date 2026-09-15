use anyhow::Context;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::delegation::ports::{SessionStore, Worker, WorkerThread};

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

pub fn delegate(
    worker: &dyn Worker,
    store: &dyn SessionStore,
    request: &WorkerRequest,
) -> anyhow::Result<(SessionId, WorkerResponse)> {
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

    let shutdown_result: anyhow::Result<()> = thread.shutdown();
    let response: WorkerResponse =
        result.with_context(|| format!("session: {} failed", session.session_id.0))?;
    shutdown_result?;
    Ok((session.session_id, response))
}

pub fn resume(
    worker: &dyn Worker,
    store: &dyn SessionStore,
    session_id: &SessionId,
    request: &WorkerRequest,
) -> anyhow::Result<(SessionId, WorkerResponse)> {
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

    let shutdown_result: anyhow::Result<()> = thread.shutdown();
    let response: WorkerResponse =
        result.with_context(|| format!("session: {} failed", session.session_id.0))?;
    shutdown_result?;
    Ok((session.session_id, response))
}
