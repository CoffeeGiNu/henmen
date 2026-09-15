use crate::delegation::application::{Session, SessionId, ThreadId, WorkerRequest, WorkerResponse};

pub trait SessionStore {
    fn save(&self, session: &Session) -> anyhow::Result<()>;
    fn load(&self, id: &SessionId) -> anyhow::Result<Session>;
}

pub trait Worker {
    fn start(&self, request: &WorkerRequest) -> anyhow::Result<Box<dyn WorkerThread>>;
    fn resume(
        &self,
        thread_id: &ThreadId,
        request: &WorkerRequest,
    ) -> anyhow::Result<Box<dyn WorkerThread>>;
}

pub trait WorkerThread {
    fn thread_id(&self) -> &ThreadId;
    fn turn(&mut self, request: &WorkerRequest) -> anyhow::Result<WorkerResponse>;
    fn shutdown(&mut self) -> anyhow::Result<()>;
}
