# walchan

A persistent channel backed by a write-ahead log (WAL).

`walchan` provides `channel` and `sync_channel` APIs intentionally close to
`std::sync::mpsc`, while persisting messages to disk and providing
**at-least-once delivery** across process restarts.

This crate is useful when you want a simple, local, crash-resilient message
queue with familiar channel semantics.

---

## Key semantics

- **Persistence**: every `send()` appends to a WAL file.
- **At-least-once delivery**: messages delivered but not yet acknowledged may be
  re-delivered after a crash.
- **RAII acknowledgement**: receiving yields a `Delivery<T>`. Dropping it
  acknowledges the message.
- **Commit batching**: receiver-side progress commits can be batched for higher
  throughput.

---

## Feature flags

- `fsync`  
  Enables calling `File::sync_data()` for real durability.
  Without this feature, `sync_data()` is compiled as a no-op and durable modes
  degrade to “flush-only” behavior.

- `stress`  
  Enables stress tests.

Enable features explicitly:

```bash
cargo add walchan
# or
cargo add walchan --features fsync
```

## Usage

### 1. Unbounded channel  
(like `std::sync::mpsc::channel`)

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Msg {
    topic: String,
    code: u32,
    payload: Vec<u8>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = walchan::channel::<Msg>("./data/demo.wal")?;

    tx.send(Msg {
        topic: "metrics.cpu".to_string(),
        code: 200,
        payload: vec![1, 2, 3, 4],
    })?;

    drop(tx); // close channel

    let d = rx.recv()?;
    assert_eq!(
        *d,
        Msg {
            topic: "metrics.cpu".to_string(),
            code: 200,
            payload: vec![1, 2, 3, 4],
        }
    );

    // `d` is acknowledged when dropped.
    Ok(())
}
````

---

### 2. Bounded channel with timeout send

(like `std::sync::mpsc::sync_channel`)

```rust
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Bin(Vec<u8>);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = walchan::sync_channel::<Bin>("./data/bounded.wal", 8)?;

    tx.send_timeout(
        Bin(vec![0xde, 0xad, 0xbe, 0xef]),
        Duration::from_secs(1),
    )?;

    let d = rx.recv()?;
    assert_eq!(*d, Bin(vec![0xde, 0xad, 0xbe, 0xef]));

    Ok(())
}
```

---

### 3. Receiver commit batching

(throughput vs. re-delivery window)

```rust
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Event {
    topic: String,
    code: u32,
    payload: Vec<u8>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = walchan::channel::<Event>("./data/events.wal")?;

    // Commit progress in batches for higher throughput.
    // Trade-off: after a crash, up to ~commit_every messages may be re-delivered.
    let rx = rx.with_options(walchan::ReceiverOptions {
        commit_every: 64,
        commit_max_delay: Duration::from_millis(5),
    });

    tx.send(Event {
        topic: "metrics.cpu".to_string(),
        code: 200,
        payload: vec![1, 2, 3, 4],
    })?;
    tx.send(Event {
        topic: "metrics.mem".to_string(),
        code: 201,
        payload: vec![9, 8, 7],
    })?;
    drop(tx);

    let a = rx.recv()?;
    let b = rx.recv()?;

    assert_eq!(a.topic, "metrics.cpu");
    assert_eq!(b.topic, "metrics.mem");

    Ok(())
}
```
---

## API overview

### Channel creation

- `channel(path) -> (Sender<T>, Receiver<T>)`
- `sync_channel(path, bound) -> (SyncSender<T>, Receiver<T>)`

### Sending

- `Sender::send`
- `SyncSender::{send, try_send, send_timeout}`

### Receiving

- `Receiver::{recv, try_recv, recv_timeout, recv_deadline}`
- `Receiver::try_iter`
- `Receiver` implements `IntoIterator`

### Receiving yields `Delivery<T>`

- `*delivery` / `delivery.get()` to access the payload
- Dropping the `Delivery` acknowledges it

---

## Durability

Durability is configured via `Options`:

- `Durability::SyncPerSend`  
  Flush + fsync on every send (strongest, slowest)

- `Durability::FlushOnly`  
  Flush only (fastest)

- `Durability::SyncEvery { max_ops, max_delay }`  
  Periodic fsync

> Note: without the `fsync` feature enabled, fsync calls are no-ops.

---

## Caveats

- **Delivery is at-least-once**.  
  Your application must tolerate duplicates (e.g. via idempotency).

- **Acknowledgement is RAII-based**.  
  If the process crashes before a `Delivery` is dropped and committed, the
  message may be re-delivered.

- `walchan` is a simple local WAL-backed channel, not a distributed broker.

---

## License

MIT
