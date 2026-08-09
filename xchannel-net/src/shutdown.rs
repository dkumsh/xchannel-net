//! Graceful shutdown on `SIGTERM` / `SIGINT`.
//!
//! **This is courtesy, not safety.** A hard kill of `xchanneld` is already safe: the daemon is
//! never in its writers' path (no-custody, `DESIGN.md` §5), committed records are durable in
//! xchannel's mmap, per-member merge cursors are *recomputed* from the topic's own log rather than
//! saved, and a subscriber resumes from its replica's own head. The cross-process tests `SIGKILL`
//! the daemon and assert every member resumes contiguously. Nothing here exists to prevent
//! corruption, because there is none to prevent.
//!
//! What it buys is **promptness**. Without it, peers only notice a departure when the heartbeat
//! liveness timeout expires — ten seconds during which they still believe this node's channels are
//! reachable and keep trying to replicate from them. Saying "leaving" collapses that to one round
//! trip.
//!
//! No `libc` dependency: the project has none beyond xchannel, and `signal(2)` is two lines to
//! declare against the C runtime Rust already links.

use std::sync::atomic::{AtomicBool, Ordering};

/// Set by the signal handler; read by the main thread.
static REQUESTED: AtomicBool = AtomicBool::new(false);

// Only the two signals a supervisor actually sends. Values are identical on Linux and the BSDs,
// and this daemon is Unix-only regardless (`0700` data dirs, a Unix-socket client plane).
const SIGINT: i32 = 2;
const SIGTERM: i32 = 15;

unsafe extern "C" {
    fn signal(signum: i32, handler: usize) -> usize;
}

/// The handler. A relaxed store to a `static` is one of the few things that is safe to do in a
/// signal handler — no allocation, no locks, no reentrancy.
extern "C" fn on_signal(_sig: i32) {
    REQUESTED.store(true, Ordering::Relaxed);
}

/// Install handlers for `SIGTERM` and `SIGINT`. Idempotent; failures are ignored, in which case the
/// daemon simply keeps the default behaviour of dying immediately — which, per the note above, is
/// safe.
pub fn install() {
    for sig in [SIGINT, SIGTERM] {
        // SAFETY: `on_signal` is `extern "C"`, takes the right argument, and does nothing but a
        // relaxed atomic store.
        unsafe { signal(sig, on_signal as usize) };
    }
}

/// Whether a shutdown has been requested.
pub fn requested() -> bool {
    REQUESTED.load(Ordering::Relaxed)
}

/// Block until a shutdown is requested, polling at `interval`.
///
/// Polling rather than parking, because a signal handler cannot safely signal a condvar — it may
/// not take a lock. `interval` is therefore the worst-case delay between the signal arriving and
/// the daemon acting on it.
pub fn wait(interval: std::time::Duration) {
    while !requested() {
        std::thread::sleep(interval);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The flag starts clear and the handler sets it. Invoked directly rather than by raising a
    /// real signal, so the test cannot disturb the harness running it.
    #[test]
    fn the_handler_sets_the_flag() {
        assert!(!requested(), "nothing has asked us to stop");
        on_signal(SIGTERM);
        assert!(requested());
        // Leave it clear for any other test in this binary.
        REQUESTED.store(false, Ordering::Relaxed);
    }
}
