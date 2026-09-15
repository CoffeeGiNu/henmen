use std::path::PathBuf;

use crate::delegation::{
    application::{Session, SessionId},
    ports::SessionStore,
};

pub struct SessionFiles {
    root: PathBuf,
}

impl SessionFiles {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn path(&self, id: &SessionId) -> PathBuf {
        self.root.join(format!("{}.json", id.as_str()))
    }
}

impl SessionStore for SessionFiles {
    fn save(&self, session: &Session) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.root)?;
        let path: PathBuf = self.path(&session.session_id);
        let temporary_path: PathBuf = path.with_extension("json.tmp");
        std::fs::write(&temporary_path, serde_json::to_vec_pretty(session)?)?;
        std::fs::rename(&temporary_path, &path)?;
        Ok(())
    }

    fn load(&self, id: &SessionId) -> anyhow::Result<Session> {
        let bytes: Vec<u8> = std::fs::read(self.path(id))?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delegation::application::{Mode, SessionStatus, ThreadId};

    fn sample() -> Session {
        Session {
            session_id: "01K5CQXM8N7VZR3TFWJ0HB2YQD"
                .parse()
                .expect("valid session id"),
            thread_id: ThreadId("thread_abc".to_string()),
            cwd: PathBuf::from("/home/coffeeginu/henmen"),
            model: "gpt-5.6-luna".to_string(),
            effort: "max".to_string(),
            mode: Mode::Inspect,
            status: SessionStatus::Running,
        }
    }

    #[test]
    fn saves_and_loads_roundtrip() -> anyhow::Result<()> {
        let root: PathBuf =
            std::env::temp_dir().join(format!("henmen-test-{}", std::process::id()));
        let store: SessionFiles = SessionFiles::new(root.clone());
        let session: Session = sample();

        store.save(&session)?;

        let loaded: Session = store.load(&session.session_id)?;
        assert_eq!(&loaded, &session);

        let temporary_path: PathBuf = store.path(&session.session_id).with_extension("json.tmp");
        assert!(!temporary_path.exists());

        std::fs::remove_dir_all(&root)?;
        Ok(())
    }
}
