//! kqueue-based watcher for `chat.db-wal`. Every SQLite WAL commit is a write
//! to this file, so an `EVFILT_VNODE` subscription on its fd is the closest an
//! outside process can get to "notify me on every commit" — and unlike FSEvents,
//! kqueue keeps a pending event flagged until consumed, so it doesn't silently
//! drop notifications. Coalescing is fine: our caller range-scans all new rows.
//!
//! WAL lifecycle: on checkpoint, SQLite may delete/recreate the file (new inode).
//! That surfaces as NOTE_DELETE/NOTE_RENAME; we close the stale fd and re-open.

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::ptr;
use std::time::Duration;

/// Why `wait` returned.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WalEvent {
    /// The WAL was written/extended — new frames to read.
    Written,
    /// The WAL was deleted/renamed (checkpoint) and we re-armed onto the new one.
    Recreated,
    /// The backstop timeout elapsed with no kqueue event.
    Timeout,
}

pub struct WalWatcher {
    kq: i32,
    wal_path: PathBuf,
    wal_fd: Option<i32>,
}

// fflags we care about on the WAL vnode.
const WATCH_FFLAGS: u32 = libc::NOTE_WRITE
    | libc::NOTE_EXTEND
    | libc::NOTE_DELETE
    | libc::NOTE_RENAME
    | libc::NOTE_REVOKE;

impl WalWatcher {
    pub fn new(wal_path: PathBuf) -> std::io::Result<WalWatcher> {
        let kq = unsafe { libc::kqueue() };
        if kq < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut w = WalWatcher {
            kq,
            wal_path,
            wal_fd: None,
        };
        w.try_arm();
        Ok(w)
    }

    /// Open the WAL (if not already) and register it with kqueue. No-op if the
    /// file doesn't exist yet (we retry on the next `wait`).
    fn try_arm(&mut self) {
        if self.wal_fd.is_some() {
            return;
        }
        let cpath = match CString::new(self.wal_path.as_os_str().as_bytes()) {
            Ok(c) => c,
            Err(_) => return,
        };
        // O_EVTONLY: open purely for event monitoring — doesn't pin the file
        // against unlink and needs minimal access.
        let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_EVTONLY) };
        if fd < 0 {
            return; // WAL not present yet
        }
        let kev = libc::kevent {
            ident: fd as usize,
            filter: libc::EVFILT_VNODE,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: WATCH_FFLAGS,
            data: 0,
            udata: ptr::null_mut(),
        };
        let r = unsafe { libc::kevent(self.kq, &kev, 1, ptr::null_mut(), 0, ptr::null()) };
        if r < 0 {
            unsafe { libc::close(fd) };
            return;
        }
        self.wal_fd = Some(fd);
        tracing::info!(path = %self.wal_path.display(), "kqueue armed on WAL");
    }

    fn disarm(&mut self) {
        if let Some(fd) = self.wal_fd.take() {
            unsafe { libc::close(fd) };
        }
    }

    /// Block until the WAL changes or `timeout` elapses.
    pub fn wait(&mut self, timeout: Duration) -> WalEvent {
        // Pick up the WAL if it appeared (or reappeared) since last call.
        self.try_arm();

        let ts = libc::timespec {
            tv_sec: timeout.as_secs() as libc::time_t,
            tv_nsec: timeout.subsec_nanos() as libc::c_long,
        };
        let mut ev = libc::kevent {
            ident: 0,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            udata: ptr::null_mut(),
        };
        let n = unsafe { libc::kevent(self.kq, ptr::null(), 0, &mut ev, 1, &ts) };

        if n <= 0 {
            // 0 = timeout; <0 = EINTR/error — treat as a backstop tick.
            return WalEvent::Timeout;
        }

        // Checkpoint deleted/renamed the WAL: drop the stale fd and re-arm onto
        // whatever exists now.
        if ev.fflags & (libc::NOTE_DELETE | libc::NOTE_RENAME | libc::NOTE_REVOKE) != 0 {
            self.disarm();
            self.try_arm();
            return WalEvent::Recreated;
        }
        WalEvent::Written
    }
}

impl Drop for WalWatcher {
    fn drop(&mut self) {
        self.disarm();
        if self.kq >= 0 {
            unsafe { libc::close(self.kq) };
        }
    }
}
