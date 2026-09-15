use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

#[derive(Debug, Default)]
pub struct Termination {
    requested: AtomicBool,
    child_process_id: AtomicU32,
}

impl Termination {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn request(&self) {
        self.requested.store(true, Ordering::SeqCst);
    }

    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    pub fn set_child_process_id(&self, process_id: u32) {
        self.child_process_id.store(process_id, Ordering::SeqCst);
    }

    pub fn clear_child_process_id(&self) {
        self.child_process_id.store(0, Ordering::SeqCst);
    }

    pub fn child_process_id(&self) -> Option<u32> {
        match self.child_process_id.load(Ordering::SeqCst) {
            0 => None,
            process_id => Some(process_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_request_and_child_process_id() {
        let termination: Termination = Termination::new();
        assert!(!termination.is_requested());
        assert_eq!(termination.child_process_id(), None);

        termination.set_child_process_id(4321);
        termination.request();
        assert!(termination.is_requested());
        assert_eq!(termination.child_process_id(), Some(4321));

        termination.clear_child_process_id();
        assert_eq!(termination.child_process_id(), None);
        assert!(termination.is_requested());
    }
}
