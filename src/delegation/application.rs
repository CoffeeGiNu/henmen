use anyhow::Context;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Instant, SystemTime};

use crate::delegation::ports::{RunLog, SessionStore, Worker, WorkerThread};

#[derive(Debug, thiserror::Error)]
#[error("the worker was interrupted")]
pub struct Interrupted;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SessionId(String);

impl SessionId {
    pub fn new() -> Self {
        Self(ulid::Ulid::generate().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for SessionId {
    type Err = ulid::DecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed_ulid: ulid::Ulid = ulid::Ulid::from_string(value)?;
        Ok(Self(parsed_ulid.to_string()))
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<String> for SessionId {
    type Error = ulid::DecodeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<SessionId> for String {
    fn from(session_id: SessionId) -> Self {
        session_id.0
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
pub enum RunKind {
    Delegate,
    Resume,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkerMetrics {
    pub usage: Option<TokenUsage>,
    pub startup_milliseconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    pub timestamp_start: u64,
    pub timestamp_end: u64,
    #[serde(rename = "duration_ms")]
    pub duration_milliseconds: u64,
    pub kind: RunKind,
    pub session_id: SessionId,
    pub model: String,
    pub effort: String,
    pub mode: Mode,
    pub session_status: SessionStatus,
    pub worker_status: Option<WorkerStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
    #[serde(rename = "worker_startup_ms", skip_serializing_if = "Option::is_none")]
    pub worker_startup_milliseconds: Option<u64>,
}

fn epoch_milliseconds(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn write_run_record(
    run_log: Option<&dyn RunLog>,
    kind: RunKind,
    session: &Session,
    metrics: &WorkerMetrics,
    worker_status: Option<WorkerStatus>,
    start: (SystemTime, Instant),
) {
    let Some(run_log) = run_log else {
        return;
    };
    let record: RunRecord = RunRecord {
        timestamp_start: epoch_milliseconds(start.0),
        timestamp_end: epoch_milliseconds(SystemTime::now()),
        duration_milliseconds: start.1.elapsed().as_millis() as u64,
        kind,
        session_id: session.session_id.clone(),
        model: session.model.clone(),
        effort: session.effort.clone(),
        mode: session.mode,
        session_status: session.status,
        worker_status,
        usage: metrics.usage,
        worker_startup_milliseconds: metrics.startup_milliseconds,
    };
    if let Err(error) = run_log.append(&record) {
        eprintln!("henmen: could not write the log entry: {error}");
    }
}

fn run_turn(
    thread: &mut dyn WorkerThread,
    store: &dyn SessionStore,
    run_log: Option<&dyn RunLog>,
    kind: RunKind,
    session: &mut Session,
    request: &WorkerRequest,
    start: (SystemTime, Instant),
) -> anyhow::Result<WorkerResponse> {
    store.save(session)?;

    let result: anyhow::Result<WorkerResponse> = thread.turn(request);
    session.status = match &result {
        Ok(_) => SessionStatus::Completed,
        Err(error) if error.downcast_ref::<Interrupted>().is_some() => SessionStatus::Interrupted,
        Err(_) => SessionStatus::Failed,
    };
    store.save(session)?;
    let worker_status: Option<WorkerStatus> = result.as_ref().ok().map(|response| response.status);
    write_run_record(
        run_log,
        kind,
        session,
        &thread.metrics(),
        worker_status,
        start,
    );
    result
}

fn shutdown_after<T>(
    thread: &mut dyn WorkerThread,
    result: anyhow::Result<T>,
) -> anyhow::Result<T> {
    let shutdown_result: anyhow::Result<()> = thread.shutdown();
    match (result, shutdown_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(shutdown_error)) => Err(shutdown_error),
        (Err(operation_error), Ok(())) => Err(operation_error),
        (Err(operation_error), Err(shutdown_error)) => {
            let combined: String = format!(
                "{operation_error:#} (additionally, the worker shutdown failed: {shutdown_error:#})"
            );
            Err(operation_error.context(combined))
        }
    }
}

pub fn delegate(
    worker: &dyn Worker,
    store: &dyn SessionStore,
    run_log: Option<&dyn RunLog>,
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

    let result: anyhow::Result<WorkerResponse> = run_turn(
        thread.as_mut(),
        store,
        run_log,
        RunKind::Delegate,
        &mut session,
        request,
        start,
    )
    .with_context(|| format!("session: {} failed", session.session_id));
    let response: WorkerResponse = shutdown_after(thread.as_mut(), result)?;
    Ok((session.session_id, response))
}

pub fn resume(
    worker: &dyn Worker,
    store: &dyn SessionStore,
    run_log: Option<&dyn RunLog>,
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

    let result: anyhow::Result<WorkerResponse> = run_turn(
        thread.as_mut(),
        store,
        run_log,
        RunKind::Resume,
        &mut session,
        request,
        start,
    )
    .with_context(|| format!("session: {} failed", session.session_id));
    let response: WorkerResponse = shutdown_after(thread.as_mut(), result)?;
    Ok((session.session_id, response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::path::PathBuf;
    use std::rc::Rc;

    #[derive(Clone, Copy)]
    enum SaveFailure {
        Never,
        First,
        Second,
    }

    struct FakeSessionStore {
        save_failure: SaveFailure,
        save_count: Cell<usize>,
        loaded_session: Session,
    }

    impl FakeSessionStore {
        fn new(save_failure: SaveFailure) -> Self {
            Self {
                save_failure,
                save_count: Cell::new(0),
                loaded_session: fake_session(),
            }
        }
    }

    impl SessionStore for FakeSessionStore {
        fn save(&self, _session: &Session) -> anyhow::Result<()> {
            let save_count: usize = self.save_count.get() + 1;
            self.save_count.set(save_count);
            let should_fail: bool = match self.save_failure {
                SaveFailure::Never => false,
                SaveFailure::First => save_count == 1,
                SaveFailure::Second => save_count == 2,
            };
            if should_fail {
                anyhow::bail!("the fake store refused to save");
            }
            Ok(())
        }

        fn load(&self, _id: &SessionId) -> anyhow::Result<Session> {
            Ok(self.loaded_session.clone())
        }
    }

    struct FakeWorker {
        shutdown_calls: Rc<Cell<usize>>,
        fail_turn: bool,
        fail_shutdown: bool,
    }

    impl FakeWorker {
        fn new(shutdown_calls: Rc<Cell<usize>>, fail_turn: bool, fail_shutdown: bool) -> Self {
            Self {
                shutdown_calls,
                fail_turn,
                fail_shutdown,
            }
        }

        fn make_thread(&self) -> FakeWorkerThread {
            FakeWorkerThread {
                thread_id: ThreadId("fake-thread".to_string()),
                shutdown_calls: Rc::clone(&self.shutdown_calls),
                fail_turn: self.fail_turn,
                fail_shutdown: self.fail_shutdown,
                response: fake_response(),
            }
        }
    }

    impl Worker for FakeWorker {
        fn start(&self, _request: &WorkerRequest) -> anyhow::Result<Box<dyn WorkerThread>> {
            Ok(Box::new(self.make_thread()))
        }

        fn resume(
            &self,
            _thread_id: &ThreadId,
            _request: &WorkerRequest,
        ) -> anyhow::Result<Box<dyn WorkerThread>> {
            Ok(Box::new(self.make_thread()))
        }
    }

    struct FakeWorkerThread {
        thread_id: ThreadId,
        shutdown_calls: Rc<Cell<usize>>,
        fail_turn: bool,
        fail_shutdown: bool,
        response: WorkerResponse,
    }

    impl WorkerThread for FakeWorkerThread {
        fn thread_id(&self) -> &ThreadId {
            &self.thread_id
        }

        fn turn(&mut self, _request: &WorkerRequest) -> anyhow::Result<WorkerResponse> {
            if self.fail_turn {
                anyhow::bail!("the fake worker failed the turn");
            }
            Ok(self.response.clone())
        }

        fn shutdown(&mut self) -> anyhow::Result<()> {
            let shutdown_calls: usize = self.shutdown_calls.get() + 1;
            self.shutdown_calls.set(shutdown_calls);
            if self.fail_shutdown {
                anyhow::bail!("the fake thread failed to shut down");
            }
            Ok(())
        }
    }

    fn fake_request() -> WorkerRequest {
        WorkerRequest {
            task: "the fake task".to_string(),
            cwd: PathBuf::from("/tmp"),
            model: "fake-model".to_string(),
            effort: "medium".to_string(),
            mode: Mode::Inspect,
        }
    }

    fn fake_response() -> WorkerResponse {
        WorkerResponse {
            status: WorkerStatus::Done,
            summary: "the fake worker completed the task".to_string(),
            evidence: vec!["the fake worker completed the task".to_string()],
            changed_files: Vec::new(),
            tests: Vec::new(),
            open_questions: Vec::new(),
        }
    }

    fn fake_session() -> Session {
        Session {
            session_id: "01K5CQXM8N7VZR3TFWJ0HB2YQD"
                .parse()
                .expect("valid session id"),
            thread_id: ThreadId("fake-thread".to_string()),
            cwd: PathBuf::from("/tmp"),
            model: "fake-model".to_string(),
            effort: "medium".to_string(),
            mode: Mode::Inspect,
            status: SessionStatus::Running,
        }
    }

    #[test]
    fn delegate_shuts_down_once_and_returns_the_response() {
        let shutdown_calls: Rc<Cell<usize>> = Rc::new(Cell::new(0));
        let worker: FakeWorker = FakeWorker::new(Rc::clone(&shutdown_calls), false, false);
        let store: FakeSessionStore = FakeSessionStore::new(SaveFailure::Never);
        let request: WorkerRequest = fake_request();

        let result: anyhow::Result<(SessionId, WorkerResponse)> =
            delegate(&worker, &store, None, &request);

        assert_eq!(shutdown_calls.get(), 1);
        let (_, response): (SessionId, WorkerResponse) =
            result.expect("delegate should return the fake response");
        assert_eq!(response, fake_response());
    }

    #[test]
    fn delegate_shuts_down_once_when_the_first_save_fails() {
        let shutdown_calls: Rc<Cell<usize>> = Rc::new(Cell::new(0));
        let worker: FakeWorker = FakeWorker::new(Rc::clone(&shutdown_calls), false, false);
        let store: FakeSessionStore = FakeSessionStore::new(SaveFailure::First);
        let request: WorkerRequest = fake_request();

        let result: anyhow::Result<(SessionId, WorkerResponse)> =
            delegate(&worker, &store, None, &request);

        assert_eq!(shutdown_calls.get(), 1);
        let error: anyhow::Error = result.expect_err("delegate should return the save error");
        let formatted_error: String = format!("{error:#}");
        assert!(formatted_error.contains("the fake store refused to save"));
    }

    #[test]
    fn delegate_shuts_down_once_when_the_turn_fails() {
        let shutdown_calls: Rc<Cell<usize>> = Rc::new(Cell::new(0));
        let worker: FakeWorker = FakeWorker::new(Rc::clone(&shutdown_calls), true, false);
        let store: FakeSessionStore = FakeSessionStore::new(SaveFailure::Never);
        let request: WorkerRequest = fake_request();

        let result: anyhow::Result<(SessionId, WorkerResponse)> =
            delegate(&worker, &store, None, &request);

        assert_eq!(shutdown_calls.get(), 1);
        let error: anyhow::Error = result.expect_err("delegate should return the turn error");
        let formatted_error: String = format!("{error:#}");
        assert!(formatted_error.contains("the fake worker failed the turn"));
    }

    #[test]
    fn delegate_shuts_down_once_when_the_final_save_fails() {
        let shutdown_calls: Rc<Cell<usize>> = Rc::new(Cell::new(0));
        let worker: FakeWorker = FakeWorker::new(Rc::clone(&shutdown_calls), false, false);
        let store: FakeSessionStore = FakeSessionStore::new(SaveFailure::Second);
        let request: WorkerRequest = fake_request();

        let result: anyhow::Result<(SessionId, WorkerResponse)> =
            delegate(&worker, &store, None, &request);

        assert_eq!(shutdown_calls.get(), 1);
        let error: anyhow::Error = result.expect_err("delegate should return the save error");
        let formatted_error: String = format!("{error:#}");
        assert!(formatted_error.contains("the fake store refused to save"));
    }

    #[test]
    fn delegate_returns_the_shutdown_error_after_a_successful_turn() {
        let shutdown_calls: Rc<Cell<usize>> = Rc::new(Cell::new(0));
        let worker: FakeWorker = FakeWorker::new(Rc::clone(&shutdown_calls), false, true);
        let store: FakeSessionStore = FakeSessionStore::new(SaveFailure::Never);
        let request: WorkerRequest = fake_request();

        let result: anyhow::Result<(SessionId, WorkerResponse)> =
            delegate(&worker, &store, None, &request);

        assert_eq!(shutdown_calls.get(), 1);
        let error: anyhow::Error = result.expect_err("delegate should return the shutdown error");
        let formatted_error: String = format!("{error:#}");
        assert!(formatted_error.contains("the fake thread failed to shut down"));
    }

    #[test]
    fn delegate_keeps_both_turn_and_shutdown_errors() {
        let shutdown_calls: Rc<Cell<usize>> = Rc::new(Cell::new(0));
        let worker: FakeWorker = FakeWorker::new(Rc::clone(&shutdown_calls), true, true);
        let store: FakeSessionStore = FakeSessionStore::new(SaveFailure::Never);
        let request: WorkerRequest = fake_request();

        let result: anyhow::Result<(SessionId, WorkerResponse)> =
            delegate(&worker, &store, None, &request);

        assert_eq!(shutdown_calls.get(), 1);
        let error: anyhow::Error = result.expect_err("delegate should return the turn error");
        let formatted_error: String = format!("{error:#}");
        assert!(formatted_error.contains("the fake worker failed the turn"));
        assert!(formatted_error.contains("the fake thread failed to shut down"));
        assert!(
            format!("{error}").starts_with("session: "),
            "the operation failure must stay the subject, got: {error}"
        );
        assert_eq!(
            error.root_cause().to_string(),
            "the fake worker failed the turn"
        );
    }

    #[test]
    fn resume_shuts_down_once_when_the_turn_fails() {
        let shutdown_calls: Rc<Cell<usize>> = Rc::new(Cell::new(0));
        let worker: FakeWorker = FakeWorker::new(Rc::clone(&shutdown_calls), true, false);
        let store: FakeSessionStore = FakeSessionStore::new(SaveFailure::Never);
        let request: WorkerRequest = fake_request();
        let session_id: SessionId = fake_session().session_id;

        let result: anyhow::Result<(SessionId, WorkerResponse)> =
            resume(&worker, &store, None, &session_id, &request);

        assert_eq!(shutdown_calls.get(), 1);
        let error: anyhow::Error = result.expect_err("resume should return the turn error");
        let formatted_error: String = format!("{error:#}");
        assert!(formatted_error.contains("the fake worker failed the turn"));
    }

    #[test]
    fn valid_session_id_round_trips() {
        let session_id: SessionId = "01K5CQXM8N7VZR3TFWJ0HB2YQD"
            .parse()
            .expect("valid session id");

        assert_eq!(session_id.as_str(), "01K5CQXM8N7VZR3TFWJ0HB2YQD");
        assert_eq!(session_id.to_string(), "01K5CQXM8N7VZR3TFWJ0HB2YQD");
    }

    #[test]
    fn lowercase_session_id_is_canonicalized() {
        let session_id: SessionId = "01k5cqxm8n7vzr3tfwj0hb2yqd"
            .parse()
            .expect("valid session id");

        assert_eq!(session_id.as_str(), "01K5CQXM8N7VZR3TFWJ0HB2YQD");
    }

    #[test]
    fn invalid_session_id_fails_to_parse() {
        let result: Result<SessionId, ulid::DecodeError> = "not-a-ulid".parse();

        assert!(result.is_err());
    }

    #[test]
    fn session_id_uses_a_bare_json_string() {
        let session_id: SessionId = "01K5CQXM8N7VZR3TFWJ0HB2YQD"
            .parse()
            .expect("valid session id");
        let serialized: String = serde_json::to_string(&session_id).expect("serialize session id");
        let result: Result<SessionId, serde_json::Error> = serde_json::from_str("\"not-a-ulid\"");

        assert_eq!(serialized, "\"01K5CQXM8N7VZR3TFWJ0HB2YQD\"");
        assert!(result.is_err());
    }
}
