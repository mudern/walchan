//! A persistent channel backed by a write-ahead log (WAL).
//!
//! This module provides `channel` and `sync_channel` APIs that are intentionally
//! close to [`std::sync::mpsc`], while persisting messages to disk.
//!
//! ## Key differences from `std::sync::mpsc`
//!
//! - **Persistence**: sent messages are appended to a WAL file.
//! - **At-least-once delivery**: after a crash, messages that were delivered but
//!   not yet acknowledged may be re-delivered.
//! - **RAII acknowledgement**: receiving yields a [`Delivery<T>`]. Dropping the
//!   `Delivery` acknowledges it.
//!
//! ## Feature flags
//!
//! - `fsync`: enables calling `File::sync_data()` for stronger durability.
//!   Without this feature, `sync_data()` becomes a no-op and durability modes
//!   degrade to “flush-only” behavior (see [`Durability`]).
//!
//! ## Examples
//!
//! Basic unbounded channel:
//!
//! ```no_run
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
//! struct Msg(u64);
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let (tx, rx) = walchan::channel::<Msg>("./data/demo.wal")?;
//!
//! tx.send(Msg(1))?;
//! drop(tx); // close channel
//!
//! let d = rx.recv()?;
//! assert_eq!(*d, Msg(1));
//! // `d` is acknowledged when dropped.
//! # Ok(()) }
//! ```
//!
//! Bounded channel with timeout send (binary payload):
//!
//! ```no_run
//! use serde::{Deserialize, Serialize};
//! use std::time::Duration;
//!
//! #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
//! struct Msg(Vec<u8>);
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let (tx, rx) = walchan::sync_channel::<Msg>("./data/bounded.wal", 8)?;
//!
//! tx.send_timeout(Msg(vec![0xde, 0xad, 0xbe, 0xef]), Duration::from_secs(1))?;
//! let d = rx.recv()?;
//! assert_eq!(*d, Msg(vec![0xde, 0xad, 0xbe, 0xef]));
//! # Ok(()) }
//! ```
//!
//! Structured messages and receiver commit batching:
//!
//! ```no_run
//! use serde::{Deserialize, Serialize};
//! use std::time::Duration;
//!
//! #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
//! struct Event {
//!     topic: String,
//!     code: u32,
//!     payload: Vec<u8>,
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let (tx, rx) = walchan::channel::<Event>("./data/events.wal")?;
//!
//! let rx = rx.with_options(walchan::ReceiverOptions {
//!     commit_every: 64,
//!     commit_max_delay: Duration::from_millis(5),
//! });
//!
//! tx.send(Event {
//!     topic: "metrics.cpu".to_string(),
//!     code: 200,
//!     payload: vec![1, 2, 3, 4],
//! })?;
//! drop(tx);
//!
//! let d = rx.recv()?;
//! assert_eq!(
//!     *d,
//!     Event {
//!         topic: "metrics.cpu".to_string(),
//!         code: 200,
//!         payload: vec![1, 2, 3, 4],
//!     }
//! );
//! # Ok(()) }
//! ```

use parking_lot::{Condvar, Mutex};
use serde::{Serialize, de::DeserializeOwned};
use std::io::Write;
use std::{
    fs::OpenOptions,
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use crate::wal;

/// Public error type (WAL-layer errors, including codec failures).
pub use crate::wal::WalError as Error;

/// The error returned by [`Receiver::try_recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is currently empty.
    Empty,
    /// All senders have been dropped.
    Disconnected,
}

impl std::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "no message available"),
            TryRecvError::Disconnected => write!(f, "channel disconnected"),
        }
    }
}
impl std::error::Error for TryRecvError {}

/// The error returned by [`Receiver::recv_timeout`] and [`Receiver::recv_deadline`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvTimeoutError {
    /// The timeout elapsed before a message became available.
    Timeout,
    /// All senders have been dropped.
    Disconnected,
}

impl std::fmt::Display for RecvTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvTimeoutError::Timeout => write!(f, "timed out waiting on channel"),
            RecvTimeoutError::Disconnected => write!(f, "channel disconnected"),
        }
    }
}
impl std::error::Error for RecvTimeoutError {}

/// The error returned by [`Receiver::recv`].
#[derive(Debug)]
pub struct RecvError;

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "channel disconnected")
    }
}
impl std::error::Error for RecvError {}

/// The kind of failure that occurred when sending.
#[derive(Debug)]
pub enum SendErrorKind {
    /// The receiver side has been dropped.
    Disconnected,
    /// A WAL append/flush/fsync failure.
    Io(std::io::Error),
}

/// Error returned from [`Sender::send`] / [`SyncSender::send`].
///
/// Unlike `std::sync::mpsc::SendError`, this error may also represent
/// filesystem I/O failures.
#[derive(Debug)]
pub struct SendError<T> {
    /// The message that could not be sent.
    pub value: T,
    /// The reason sending failed.
    pub kind: SendErrorKind,
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            SendErrorKind::Disconnected => write!(f, "send failed: disconnected"),
            SendErrorKind::Io(e) => write!(f, "send failed: io error: {e}"),
        }
    }
}
impl<T: std::fmt::Debug + 'static> std::error::Error for SendError<T> {}

/// The error returned by [`SyncSender::try_send`].
#[derive(Debug)]
pub enum TrySendError<T> {
    /// The channel is full.
    Full(T),
    /// The receiver side has been dropped.
    Disconnected(T),
}

impl<T> TrySendError<T> {
    /// Returns the message that could not be sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(v) => v,
            TrySendError::Disconnected(v) => v,
        }
    }

    /// Returns `true` if the receiver side has been dropped.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, TrySendError::Disconnected(_))
    }
}

impl<T> std::fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "try_send failed: channel is full"),
            TrySendError::Disconnected(_) => write!(f, "try_send failed: disconnected"),
        }
    }
}
impl<T: std::fmt::Debug + 'static> std::error::Error for TrySendError<T> {}

/// Error returned from [`SyncSender::send_timeout`].
#[derive(Debug, PartialEq, Eq)]
pub enum SendTimeoutError<T> {
    /// The channel could not accept the message before the timeout elapsed.
    Timeout(T),
    /// The receiver side has been dropped.
    Disconnected(T),
}

impl<T> SendTimeoutError<T> {
    /// Returns the message that could not be sent.
    pub fn into_inner(self) -> T {
        match self {
            SendTimeoutError::Timeout(v) => v,
            SendTimeoutError::Disconnected(v) => v,
        }
    }

    /// Returns `true` if the receiver side has been dropped.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, SendTimeoutError::Disconnected(_))
    }
}

impl<T> std::fmt::Display for SendTimeoutError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendTimeoutError::Timeout(_) => write!(f, "send timed out"),
            SendTimeoutError::Disconnected(_) => write!(f, "channel disconnected"),
        }
    }
}
impl<T: std::fmt::Debug + 'static> std::error::Error for SendTimeoutError<T> {}

/// WAL durability options.
///
/// Note: without the `fsync` feature, `sync_data()` calls are compiled as no-ops,
/// so `SyncPerSend` and `SyncEvery` behave like “flush-only” in practice.
#[derive(Debug, Clone)]
pub enum Durability {
    /// Flush and `fsync` on every send.
    SyncPerSend,
    /// Flush only (no `fsync`).
    FlushOnly,
    /// Flush every send, and `fsync` periodically.
    SyncEvery { max_ops: usize, max_delay: Duration },
}

/// Channel/WAL open options.
#[derive(Debug, Clone)]
pub struct Options {
    /// Writer durability behavior.
    pub durability: Durability,
    /// Whether to truncate a corrupted/partial tail during recovery.
    pub truncate_corrupt_tail: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            durability: Durability::SyncPerSend,
            truncate_corrupt_tail: true,
        }
    }
}

/// Receiver-side commit batching options.
///
/// Acknowledgement is RAII-based (dropping [`Delivery`]).
///
/// Progress is persisted by writing the latest acknowledged "next offset" into
/// the WAL header. With batching enabled, the receiver may persist less often,
/// which increases throughput but increases re-delivery after a crash.
#[derive(Debug, Clone)]
pub struct ReceiverOptions {
    /// Commit after this many acknowledged deliveries.
    ///
    /// Default: `1` (commit every message).
    pub commit_every: usize,
    /// Commit if this duration has elapsed since the last commit.
    ///
    /// Default: `Duration::ZERO` (disabled).
    pub commit_max_delay: Duration,
}

impl Default for ReceiverOptions {
    fn default() -> Self {
        Self {
            commit_every: 1,
            commit_max_delay: Duration::ZERO,
        }
    }
}

struct WriterSync {
    options: Options,
    last_sync: Instant,
    unsynced_ops: usize,
}

impl WriterSync {
    fn new(options: Options) -> Self {
        Self {
            options,
            last_sync: Instant::now(),
            unsynced_ops: 0,
        }
    }

    fn after_append(&mut self, w: &mut std::fs::File) -> io::Result<()> {
        match &self.options.durability {
            Durability::SyncPerSend => {
                w.flush()?;
                wal::fsync_data(w)?;
                self.last_sync = Instant::now();
                self.unsynced_ops = 0;
            }
            Durability::FlushOnly => {
                w.flush()?;
                self.unsynced_ops = self.unsynced_ops.saturating_add(1);
            }
            Durability::SyncEvery { max_ops, max_delay } => {
                w.flush()?;
                self.unsynced_ops = self.unsynced_ops.saturating_add(1);
                if self.unsynced_ops >= *max_ops || self.last_sync.elapsed() >= *max_delay {
                    wal::fsync_data(w)?;
                    self.last_sync = Instant::now();
                    self.unsynced_ops = 0;
                }
            }
        }
        Ok(())
    }
}

struct Inner {
    /// Single file used for both header commits and record appends.
    ///
    /// The writer does not rely on `O_APPEND`; it seeks explicitly.
    writer: Mutex<std::fs::File>,
    writer_sync: Mutex<WriterSync>,

    wal_len: Mutex<u64>,
    recv_cv: Condvar,

    // Bounded-channel bookkeeping.
    bound: Option<usize>,
    inflight: Mutex<usize>,
    send_cv: Condvar,

    sender_cnt: AtomicUsize,
    receiver_alive: AtomicBool,

    /// Serializes header commits (two-slot update).
    commit_lock: Mutex<()>,
}

impl Inner {
    fn no_senders(&self) -> bool {
        self.sender_cnt.load(Ordering::SeqCst) == 0
    }

    fn receiver_gone(&self) -> bool {
        !self.receiver_alive.load(Ordering::SeqCst)
    }

    fn inc_inflight(&self) {
        if self.bound.is_some() {
            let mut n = self.inflight.lock();
            *n = n.saturating_add(1);
        }
    }

    fn dec_inflight(&self) {
        if self.bound.is_some() {
            let mut n = self.inflight.lock();
            *n = n.saturating_sub(1);
            self.send_cv.notify_one();
        }
    }
}

/// Receiver-side shared state used by RAII acknowledgements.
pub(crate) struct ReceiverState {
    inner: Arc<Inner>,
    committed_generation: Mutex<u64>,
    last_acked: Mutex<Option<u64>>,
    pending_acks: Mutex<usize>,
    last_commit_at: Mutex<Instant>,
    receiver_opts: ReceiverOptions,
}

impl ReceiverState {
    fn new(inner: Arc<Inner>, committed_generation: u64, receiver_opts: ReceiverOptions) -> Self {
        let mut ro = receiver_opts;
        if ro.commit_every == 0 {
            ro.commit_every = 1;
        }

        Self {
            inner,
            committed_generation: Mutex::new(committed_generation),
            last_acked: Mutex::new(None),
            pending_acks: Mutex::new(0),
            last_commit_at: Mutex::new(Instant::now()),
            receiver_opts: ro,
        }
    }

    /// Best-effort acknowledgement hook.
    ///
    /// This method must not panic; it is invoked from `Drop` paths.
    fn on_ack(&self, next_offset: u64, force: bool) -> Result<(), Error> {
        // Free bounded capacity as soon as the delivery is acknowledged.
        self.inner.dec_inflight();

        *self.last_acked.lock() = Some(next_offset);

        let mut pending = self.pending_acks.lock();
        *pending = pending.saturating_add(1);

        let now = Instant::now();
        let mut last_commit_at = self.last_commit_at.lock();

        let time_enabled = self.receiver_opts.commit_max_delay != Duration::ZERO;
        let time_due = time_enabled
            && now.duration_since(*last_commit_at) >= self.receiver_opts.commit_max_delay;

        let need_commit = force || *pending >= self.receiver_opts.commit_every || time_due;
        if !need_commit {
            return Ok(());
        }

        let off = match self.last_acked.lock().take() {
            Some(v) => v,
            None => {
                *pending = 0;
                *last_commit_at = now;
                return Ok(());
            }
        };

        let _g = self.inner.commit_lock.lock();
        let mut wf = self.inner.writer.lock();

        let prev_generation = *self.committed_generation.lock();
        let new_generation = wal::write_committed_to_header(&mut wf, off, prev_generation)?;
        *self.committed_generation.lock() = new_generation;

        *pending = 0;
        *last_commit_at = now;
        Ok(())
    }

    fn force_commit_pending(&self) {
        let off = { *self.last_acked.lock() };
        let Some(off) = off else { return };
        let _ = self.on_ack(off, true);
    }
}

/// A delivered but not yet acknowledged message.
///
/// Dropping this value acknowledges the message.
///
/// This type dereferences to `T` for ergonomic access.
pub struct Delivery<T> {
    value: Option<T>,
    next_offset: u64,
    st: Arc<ReceiverState>,
}

impl<T> Delivery<T> {
    pub(crate) fn new(value: T, next_offset: u64, st: Arc<ReceiverState>) -> Self {
        Self {
            value: Some(value),
            next_offset,
            st,
        }
    }

    /// Returns a reference to the payload.
    pub fn get(&self) -> &T {
        self.value.as_ref().expect("delivery already taken")
    }

    /// Returns a mutable reference to the payload.
    pub fn get_mut(&mut self) -> &mut T {
        self.value.as_mut().expect("delivery already taken")
    }

    /// Consumes the delivery and returns the payload.
    ///
    /// The delivery is acknowledged when it is dropped.
    pub fn into_inner(mut self) -> T {
        self.value.take().expect("delivery already taken")
    }

    /// Copies out the payload without consuming the delivery.
    pub fn copied(&self) -> T
    where
        T: Copy,
    {
        *self.get()
    }
}

impl<T> std::ops::Deref for Delivery<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl<T> std::ops::DerefMut for Delivery<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Delivery<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Delivery").field(self.get()).finish()
    }
}

impl<T: PartialEq> PartialEq<T> for Delivery<T> {
    fn eq(&self, other: &T) -> bool {
        self.get().eq(other)
    }
}

impl<T: PartialEq> PartialEq for Delivery<T> {
    fn eq(&self, other: &Self) -> bool {
        self.get().eq(other.get())
    }
}

impl<T: Eq> Eq for Delivery<T> {}

impl<T> Drop for Delivery<T> {
    fn drop(&mut self) {
        let _ = self.st.on_ack(self.next_offset, false);
    }
}

struct SenderCore<T> {
    inner: Arc<Inner>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Clone for SenderCore<T> {
    fn clone(&self) -> Self {
        self.inner.sender_cnt.fetch_add(1, Ordering::SeqCst);
        Self {
            inner: self.inner.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Drop for SenderCore<T> {
    fn drop(&mut self) {
        if self.inner.sender_cnt.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.recv_cv.notify_all();
            self.inner.send_cv.notify_all();
        }
    }
}

/// The sending half of an unbounded channel.
///
/// This type is analogous to [`std::sync::mpsc::Sender`], but backed by a WAL file.
pub struct Sender<T> {
    core: SenderCore<T>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}

impl<T> Sender<T>
where
    T: Serialize,
{
    /// Sends a value into the channel.
    ///
    /// Returns an error if the receiver has been dropped or the WAL write fails.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.core.send_unbounded(value)
    }

    /// Returns `true` if the receiving half has been dropped.
    pub fn is_closed(&self) -> bool {
        self.core.inner.receiver_gone()
    }
}

/// The sending half of a bounded channel.
///
/// This type is analogous to [`std::sync::mpsc::SyncSender`].
pub struct SyncSender<T> {
    core: SenderCore<T>,
}

impl<T> Clone for SyncSender<T> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}

impl<T> SyncSender<T>
where
    T: Serialize,
{
    /// Sends a value, blocking until capacity is available.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.core.send_bounded_blocking(value, None)
    }

    /// Attempts to send a value without blocking.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.core.send_bounded_try(value)
    }

    /// Returns `true` if the receiving half has been dropped.
    pub fn is_closed(&self) -> bool {
        self.core.inner.receiver_gone()
    }
}

impl<T> SyncSender<T>
where
    T: Serialize + Send + 'static,
{
    /// Sends a value, blocking until `timeout` elapses.
    pub fn send_timeout(&self, value: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        let deadline = Instant::now() + timeout;
        self.send_deadline(value, deadline)
    }

    fn send_deadline(&self, mut value: T, deadline: Instant) -> Result<(), SendTimeoutError<T>> {
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let disconnected = e.is_disconnected();
                    value = e.into_inner();

                    if disconnected {
                        return Err(SendTimeoutError::Disconnected(value));
                    }

                    if Instant::now() >= deadline {
                        return Err(SendTimeoutError::Timeout(value));
                    }

                    if !self.wait_for_space_until(deadline) {
                        return Err(SendTimeoutError::Timeout(value));
                    }
                }
            }
        }
    }

    fn wait_for_space_until(&self, deadline: Instant) -> bool {
        let Some(bound) = self.core.inner.bound else {
            return true;
        };

        let mut n = self.core.inner.inflight.lock();
        loop {
            if self.core.inner.receiver_gone() {
                return true;
            }
            if *n < bound {
                return true;
            }

            let now = Instant::now();
            if now >= deadline {
                return false;
            }

            let remain = deadline.saturating_duration_since(now);
            let _ = self.core.inner.send_cv.wait_for(&mut n, remain);
        }
    }
}

impl<T> SenderCore<T>
where
    T: Serialize,
{
    fn send_unbounded(&self, value: T) -> Result<(), SendError<T>> {
        if self.inner.receiver_gone() {
            return Err(SendError {
                value,
                kind: SendErrorKind::Disconnected,
            });
        }

        let payload = match bincode::serialize(&value) {
            Ok(p) => p,
            Err(e) => {
                return Err(SendError {
                    value,
                    kind: SendErrorKind::Io(io::Error::new(io::ErrorKind::InvalidData, e)),
                });
            }
        };

        let mut w = self.inner.writer.lock();
        let wrote = match wal::write_record(&mut w, &payload) {
            Ok(n) => n,
            Err(e) => {
                return Err(SendError {
                    value,
                    kind: SendErrorKind::Io(e),
                });
            }
        };

        {
            let mut ws = self.inner.writer_sync.lock();
            if let Err(e) = ws.after_append(&mut w) {
                return Err(SendError {
                    value,
                    kind: SendErrorKind::Io(e),
                });
            }
        }

        {
            let mut len = self.inner.wal_len.lock();
            *len = (*len).max(wal::HEADER_SIZE);
            *len += wrote;
        }

        self.inner.recv_cv.notify_one();
        Ok(())
    }

    fn send_bounded_try(&self, value: T) -> Result<(), TrySendError<T>> {
        if self.inner.receiver_gone() {
            return Err(TrySendError::Disconnected(value));
        }

        let Some(bound) = self.inner.bound else {
            return Err(TrySendError::Disconnected(value));
        };

        {
            let n = self.inner.inflight.lock();
            if *n >= bound {
                return Err(TrySendError::Full(value));
            }
        }

        let payload = match bincode::serialize(&value) {
            Ok(p) => p,
            Err(_) => return Err(TrySendError::Disconnected(value)),
        };

        self.inner.inc_inflight();

        let mut w = self.inner.writer.lock();
        let wrote = match wal::write_record(&mut w, &payload) {
            Ok(n) => n,
            Err(_) => {
                self.inner.dec_inflight();
                return Err(TrySendError::Disconnected(value));
            }
        };

        {
            let mut ws = self.inner.writer_sync.lock();
            if ws.after_append(&mut w).is_err() {
                self.inner.dec_inflight();
                return Err(TrySendError::Disconnected(value));
            }
        }

        {
            let mut len = self.inner.wal_len.lock();
            *len = (*len).max(wal::HEADER_SIZE);
            *len += wrote;
        }

        self.inner.recv_cv.notify_one();
        Ok(())
    }

    fn send_bounded_blocking(
        &self,
        value: T,
        deadline: Option<Instant>,
    ) -> Result<(), SendError<T>> {
        if self.inner.receiver_gone() {
            return Err(SendError {
                value,
                kind: SendErrorKind::Disconnected,
            });
        }

        let Some(bound) = self.inner.bound else {
            return self.send_unbounded(value);
        };

        loop {
            if self.inner.receiver_gone() {
                return Err(SendError {
                    value,
                    kind: SendErrorKind::Disconnected,
                });
            }

            let mut n = self.inner.inflight.lock();
            if *n < bound {
                *n += 1;
                break;
            }

            if let Some(dl) = deadline {
                let now = Instant::now();
                if now >= dl {
                    return Err(SendError {
                        value,
                        kind: SendErrorKind::Io(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "send timed out",
                        )),
                    });
                }
                let remain = dl.saturating_duration_since(now);
                let _ = self.inner.send_cv.wait_for(&mut n, remain);
            } else {
                self.inner.send_cv.wait(&mut n);
            }
        }

        let payload = match bincode::serialize(&value) {
            Ok(p) => p,
            Err(e) => {
                self.inner.dec_inflight();
                return Err(SendError {
                    value,
                    kind: SendErrorKind::Io(io::Error::new(io::ErrorKind::InvalidData, e)),
                });
            }
        };

        let mut w = self.inner.writer.lock();
        let wrote = match wal::write_record(&mut w, &payload) {
            Ok(n) => n,
            Err(e) => {
                self.inner.dec_inflight();
                return Err(SendError {
                    value,
                    kind: SendErrorKind::Io(e),
                });
            }
        };

        {
            let mut ws = self.inner.writer_sync.lock();
            if let Err(e) = ws.after_append(&mut w) {
                self.inner.dec_inflight();
                return Err(SendError {
                    value,
                    kind: SendErrorKind::Io(e),
                });
            }
        }

        {
            let mut len = self.inner.wal_len.lock();
            *len = (*len).max(wal::HEADER_SIZE);
            *len += wrote;
        }

        self.inner.recv_cv.notify_one();
        Ok(())
    }
}

/// The receiving half of a channel.
///
/// Receiving yields a [`Delivery<T>`] which is acknowledged on drop.
pub struct Receiver<T> {
    inner: Arc<Inner>,
    st: Arc<ReceiverState>,
    reader: Mutex<std::fs::File>,
    cur_off: Mutex<u64>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Receiver<T> {
    /// Configures receiver-side commit batching.
    pub fn with_options(mut self, opts: ReceiverOptions) -> Self {
        let mut o = opts;
        if o.commit_every == 0 {
            o.commit_every = 1;
        }

        let generation = *self.st.committed_generation.lock();
        self.st = Arc::new(ReceiverState::new(self.inner.clone(), generation, o));
        self
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.receiver_alive.store(false, Ordering::SeqCst);
        self.inner.recv_cv.notify_all();
        self.inner.send_cv.notify_all();

        self.st.force_commit_pending();
    }
}

impl<T> Receiver<T>
where
    T: DeserializeOwned,
{
    /// Receives a value, blocking until one is available.
    pub fn recv(&self) -> Result<Delivery<T>, RecvError> {
        loop {
            let cur = *self.cur_off.lock();
            let wal_len = *self.inner.wal_len.lock();

            if cur < wal_len {
                let mut r = self.reader.lock();
                let (payload, next) =
                    wal::read_record_strict(&mut r, cur).map_err(|_| RecvError)?;
                let msg: T = bincode::deserialize(&payload).map_err(|_| RecvError)?;
                *self.cur_off.lock() = next;
                return Ok(Delivery::new(msg, next, self.st.clone()));
            }

            if self.inner.no_senders() {
                return Err(RecvError);
            }

            let mut len_guard = self.inner.wal_len.lock();
            self.inner.recv_cv.wait(&mut len_guard);
        }
    }

    /// Attempts to receive a value without blocking.
    pub fn try_recv(&self) -> Result<Delivery<T>, TryRecvError> {
        let cur = *self.cur_off.lock();
        let wal_len = *self.inner.wal_len.lock();

        if cur >= wal_len {
            return if self.inner.no_senders() {
                Err(TryRecvError::Disconnected)
            } else {
                Err(TryRecvError::Empty)
            };
        }

        let mut r = self.reader.lock();
        let (payload, next) =
            wal::read_record_strict(&mut r, cur).map_err(|_| TryRecvError::Disconnected)?;
        let msg: T = bincode::deserialize(&payload).map_err(|_| TryRecvError::Disconnected)?;
        *self.cur_off.lock() = next;
        Ok(Delivery::new(msg, next, self.st.clone()))
    }

    /// Receives a value, blocking until `timeout` elapses.
    pub fn recv_timeout(&self, dur: Duration) -> Result<Delivery<T>, RecvTimeoutError> {
        let deadline = Instant::now() + dur;

        loop {
            let cur = *self.cur_off.lock();
            let wal_len = *self.inner.wal_len.lock();

            if cur < wal_len {
                let mut r = self.reader.lock();
                let (payload, next) = wal::read_record_strict(&mut r, cur)
                    .map_err(|_| RecvTimeoutError::Disconnected)?;
                let msg: T =
                    bincode::deserialize(&payload).map_err(|_| RecvTimeoutError::Disconnected)?;
                *self.cur_off.lock() = next;
                return Ok(Delivery::new(msg, next, self.st.clone()));
            }

            if self.inner.no_senders() {
                return Err(RecvTimeoutError::Disconnected);
            }

            if Instant::now() >= deadline {
                return Err(RecvTimeoutError::Timeout);
            }

            let mut len_guard = self.inner.wal_len.lock();
            let remain = deadline.saturating_duration_since(Instant::now());
            let _ = self.inner.recv_cv.wait_for(&mut len_guard, remain);
        }
    }

    /// Receives a value, blocking until `deadline`.
    pub fn recv_deadline(
        &self,
        deadline: std::time::Instant,
    ) -> Result<Delivery<T>, RecvTimeoutError> {
        let now = std::time::Instant::now();
        if deadline <= now {
            return Err(RecvTimeoutError::Timeout);
        }
        self.recv_timeout(deadline - now)
    }

    /// Creates an iterator that yields all available messages without blocking.
    pub fn try_iter(&self) -> TryIter<'_, T> {
        TryIter { rx: self }
    }
}

/// An iterator over messages that are immediately available.
pub struct TryIter<'a, T> {
    rx: &'a Receiver<T>,
}

impl<'a, T> Iterator for TryIter<'a, T>
where
    T: DeserializeOwned,
{
    type Item = Delivery<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.try_recv().ok()
    }
}

impl<T> IntoIterator for Receiver<T>
where
    T: DeserializeOwned,
{
    type Item = Delivery<T>;
    type IntoIter = ReceiverIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        ReceiverIter { rx: self }
    }
}

/// An iterator that blocks on [`Receiver::recv`].
pub struct ReceiverIter<T> {
    rx: Receiver<T>,
}

impl<T> Iterator for ReceiverIter<T>
where
    T: DeserializeOwned,
{
    type Item = Delivery<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

impl<T> From<SenderCore<T>> for Sender<T> {
    fn from(core: SenderCore<T>) -> Self {
        Self { core }
    }
}

impl<T> From<SenderCore<T>> for SyncSender<T> {
    fn from(core: SenderCore<T>) -> Self {
        Self { core }
    }
}

fn open_impl<T>(
    path: impl AsRef<Path>,
    options: Options,
    bound: Option<usize>,
) -> Result<(SenderCore<T>, Receiver<T>), Error>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    let wal_path = path.as_ref().to_path_buf();
    wal::ensure_parent_dir(&wal_path)?;

    let mut writer_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&wal_path)?;

    wal::init_file_if_needed(&mut writer_file)?;

    let (committed, generation) = wal::read_committed_from_header(&mut writer_file)?;

    let repaired_len =
        wal::scan_and_recover_tail(&mut writer_file, committed, options.truncate_corrupt_tail)?;

    let inflight0 = match bound {
        None => 0usize,
        Some(_) => {
            wal::count_records_from(&wal_path, committed.max(wal::HEADER_SIZE), repaired_len)?
        }
    };

    let inner = Arc::new(Inner {
        writer: Mutex::new(writer_file),
        writer_sync: Mutex::new(WriterSync::new(options)),
        wal_len: Mutex::new(repaired_len.max(wal::HEADER_SIZE)),
        recv_cv: Condvar::new(),
        bound,
        inflight: Mutex::new(inflight0),
        send_cv: Condvar::new(),
        sender_cnt: AtomicUsize::new(1),
        receiver_alive: AtomicBool::new(true),
        commit_lock: Mutex::new(()),
    });

    let reader_file = OpenOptions::new().read(true).open(&wal_path)?;

    let st = Arc::new(ReceiverState::new(
        inner.clone(),
        generation,
        ReceiverOptions::default(),
    ));

    let tx = SenderCore {
        inner: inner.clone(),
        _phantom: std::marker::PhantomData,
    };

    let rx = Receiver {
        inner,
        st,
        reader: Mutex::new(reader_file),
        cur_off: Mutex::new(committed.max(wal::HEADER_SIZE)),
        _phantom: std::marker::PhantomData,
    };

    Ok((tx, rx))
}

/// Creates an unbounded channel backed by a WAL file.
pub fn channel<T>(path: impl AsRef<Path>) -> Result<(Sender<T>, Receiver<T>), Error>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    let (core, rx) = open_impl(path, Options::default(), None)?;
    Ok((Sender::from(core), rx))
}

/// Creates an unbounded channel with custom [`Options`].
pub fn channel_with_options<T>(
    path: impl AsRef<Path>,
    options: Options,
) -> Result<(Sender<T>, Receiver<T>), Error>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    let (core, rx) = open_impl(path, options, None)?;
    Ok((Sender::from(core), rx))
}

/// Creates a bounded channel backed by a WAL file.
pub fn sync_channel<T>(
    path: impl AsRef<Path>,
    bound: usize,
) -> Result<(SyncSender<T>, Receiver<T>), Error>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    if bound == 0 {
        return Err(Error::Codec(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bound must be > 0",
        ))));
    }

    let (core, rx) = open_impl(path, Options::default(), Some(bound))?;
    Ok((SyncSender::from(core), rx))
}
