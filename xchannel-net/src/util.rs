//! Small internal utilities.

use std::sync::{Mutex, MutexGuard};

/// Lock a `Mutex`, recovering the guard even if a previous holder panicked while holding
/// it (poisoning). We prefer availability over aborting the whole daemon on a single
/// poisoned lock: after such a panic the protected map/queue may be slightly inconsistent,
/// which is acceptable here and far better than every other thread cascading into panics
/// via `.lock().unwrap()`.
pub(crate) trait MutexExt<T> {
    fn lock_safe(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexExt<T> for Mutex<T> {
    fn lock_safe(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
