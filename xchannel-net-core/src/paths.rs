//! Where a node keeps its data, and where a client looks for it.
//!
//! Both the daemon and the client need this answer and they must agree: `Client::connect_or_spawn`
//! finds — or starts — the implicit local daemon by looking for its socket at the default path. If
//! the two ever computed it differently, zero-config startup would fail in a way that looks like
//! "no daemon running". So the answer lives here, in the crate they both depend on, once.
//!
//! **The default is per-user and persistent: `$HOME/.xchannel-net`.** It used to be
//! `/tmp/xchanneld`, which was wrong in a way worth recording so it is not re-introduced:
//!
//! * `/tmp` is `tmpfs` on most systems, so channels — memory-mapped *files* — were held in RAM.
//!   That silently contradicts the durability the whole design rests on: a power cut lost
//!   everything, and channel bytes counted against memory.
//! * `/tmp` is cleared on reboot, so a node lost its `.node_id` along with its channels and came
//!   back as a *different* node every time. Peers keep the earlier registration, so its old
//!   channels stayed owned by an id that never returned — frozen until an operator reclaimed the
//!   names. The default arranged for that on every reboot.
//! * `/tmp` has a world-writable parent and a predictable path, so the directory (or a symlink at
//!   it) could be pre-created by anyone. The daemon fails closed, but a per-user directory removes
//!   the possibility rather than surviving it.
//! * Two users on one machine collided, and the data-dir lock meant the second daemon simply exited.
//!
//! There is deliberately **no fallback** when `HOME` is unset. A silent second-choice location is
//! how data ends up somewhere nobody looks; a service with no `HOME` is exactly the deployment that
//! should be naming its data directory explicitly.

use std::ffi::OsString;
use std::io;
use std::path::PathBuf;

/// Directory name under the user's home.
pub const DATA_DIR_NAME: &str = ".xchannel-net";

/// The client-plane socket, inside the data directory — so who may drive the daemon is decided by
/// the directory's `0700` mode rather than by a port anything local could reach.
pub const CLIENT_SOCKET_NAME: &str = "client.sock";

/// The default data directory: `$HOME/.xchannel-net`.
pub fn default_data_dir() -> io::Result<PathBuf> {
    data_dir_in(std::env::var_os("HOME"))
}

/// The default client-plane socket: `$HOME/.xchannel-net/client.sock`.
///
/// A Unix socket address has a hard length limit (~104 bytes) that this has to fit inside. A very
/// deep home directory can exceed it, and `bind` says so plainly; the fix is to name a shorter path
/// explicitly rather than for this to guess one.
pub fn default_client_path() -> io::Result<PathBuf> {
    Ok(default_data_dir()?.join(CLIENT_SOCKET_NAME))
}

/// Split out from the environment lookup so it can be tested without mutating process-global state,
/// which would race every other test in the binary.
fn data_dir_in(home: Option<OsString>) -> io::Result<PathBuf> {
    match home {
        Some(h) if !h.is_empty() => Ok(PathBuf::from(h).join(DATA_DIR_NAME)),
        _ => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "HOME is not set, so there is no default data directory — set XCHANNELD_DATA_DIR \
             (and XCHANNELD_CLIENT_PATH for a client) explicitly",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_is_a_dot_directory_under_home() {
        let dir = data_dir_in(Some(OsString::from("/home/someone"))).unwrap();
        assert_eq!(dir, PathBuf::from("/home/someone/.xchannel-net"));
        assert_eq!(
            dir.join(CLIENT_SOCKET_NAME),
            PathBuf::from("/home/someone/.xchannel-net/client.sock"),
            "the socket lives inside the 0700 data dir, not beside it"
        );
    }

    /// No fallback. An unset or empty `HOME` is an error the operator has to answer, not a cue to
    /// invent a location — the previous default put durable data on a filesystem that is wiped on
    /// reboot, and nobody noticed because it looked like it worked.
    #[test]
    fn an_unset_home_is_an_error_not_a_guess() {
        for home in [None, Some(OsString::new())] {
            let err = data_dir_in(home).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::NotFound);
            assert!(err.to_string().contains("XCHANNELD_DATA_DIR"), "{err}");
        }
    }
}
