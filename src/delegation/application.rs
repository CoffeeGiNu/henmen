use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub String);

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
    pub thread_id: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerStatus {
    Done,
    Blocked,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerResponse {
    pub status: WorkerStatus,
    pub summary: String,
    pub evidence: Vec<String>,
    pub changed_files: Vec<PathBuf>,
    pub tests: String,
    pub open_questions: Vec<String>,
}
