use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use walchan as mpsc;

fn tmp_wal(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push("walchan-tests");
    p.push(format!(
        "{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    p.set_extension("wal");
    p
}

/* ---------- basic behavior ---------- */

#[test]
fn basic_send_recv_delivery() -> anyhow::Result<()> {
    let wal = tmp_wal("basic");
    let (tx, rx) = mpsc::channel::<String>(&wal)?;

    tx.send("hello".into())?;
    tx.send("world".into())?;

    assert_eq!(rx.recv()?, "hello".to_string());
    assert_eq!(rx.recv()?, "world".to_string());
    Ok(())
}

#[test]
fn iterator_recv_delivery() -> anyhow::Result<()> {
    let wal = tmp_wal("iter");
    let (tx, rx) = mpsc::channel::<i32>(&wal)?;

    for i in 0..5 {
        tx.send(i)?;
    }
    drop(tx);

    let collected: Vec<i32> = rx.into_iter().map(|d| d.into_inner()).collect();
    assert_eq!(collected, vec![0, 1, 2, 3, 4]);
    Ok(())
}

#[test]
fn try_recv_empty_and_disconnected_delivery() -> anyhow::Result<()> {
    let wal = tmp_wal("try_recv");
    let (tx, rx) = mpsc::channel::<i32>(&wal)?;

    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    drop(tx);
    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Disconnected));
    Ok(())
}

/* ---------- sender / receiver lifecycle ---------- */

#[test]
fn send_fails_after_receiver_drop() -> anyhow::Result<()> {
    let wal = tmp_wal("send_fail");
    let (tx, rx) = mpsc::channel::<String>(&wal)?;

    drop(rx);
    let err = tx.send("x".into()).unwrap_err();
    assert!(matches!(err.kind, mpsc::SendErrorKind::Disconnected));
    Ok(())
}

#[test]
fn receiver_exits_after_all_senders_dropped() -> anyhow::Result<()> {
    let wal = tmp_wal("sender_drop");
    let (tx, rx) = mpsc::channel::<i32>(&wal)?;

    tx.send(1)?;
    drop(tx);

    assert_eq!(rx.recv()?.into_inner(), 1);
    assert!(rx.recv().is_err());
    Ok(())
}

/* ---------- WAL recovery semantics (RAII ack + receiver drop commits) ---------- */

#[test]
fn delivery_drop_acks_and_receiver_drop_commits_progress() -> anyhow::Result<()> {
    let wal = tmp_wal("graceful_delivery");

    {
        let (tx, rx) = mpsc::channel::<String>(&wal)?;
        tx.send("a".into())?;
        tx.send("b".into())?;
        drop(tx);

        // Receive "a", then drop Delivery => ack.
        let d = rx.recv()?;
        assert_eq!(d.get(), "a");
        drop(d);

        // Dropping receiver should force commit pending acks.
        drop(rx);
    }

    // Reopen: should continue from "b".
    let (_tx2, rx2) = mpsc::channel::<String>(&wal)?;
    let d2 = rx2.recv()?;
    assert_eq!(d2.into_inner(), "b");
    Ok(())
}

#[test]
fn crash_does_not_commit_offset() -> anyhow::Result<()> {
    let wal = tmp_wal("crash");

    let (tx, rx) = mpsc::channel::<String>(&wal)?;
    tx.send("a".into())?;
    tx.send("b".into())?;
    drop(tx);

    // Receive "a" but do NOT drop (simulate crash before ack+commit).
    let d = rx.recv()?;
    assert_eq!(d.get(), "a");

    // Simulate process crash: Drop is NOT executed.
    std::mem::forget(d);
    std::mem::forget(rx);

    let (_tx2, rx2) = mpsc::channel::<String>(&wal)?;
    assert_eq!(rx2.recv()?.into_inner(), "a");
    assert_eq!(rx2.recv()?.into_inner(), "b");
    Ok(())
}

#[test]
fn multiple_restarts_progress_offset_monotonically() -> anyhow::Result<()> {
    let wal = tmp_wal("restart");

    {
        let (tx, rx) = mpsc::channel::<i32>(&wal)?;
        tx.send(1)?;
        tx.send(2)?;
        drop(tx);

        let d = rx.recv()?;
        assert_eq!(d.into_inner(), 1);
        drop(rx); // force commit pending acks
    }

    {
        let (_tx, rx) = mpsc::channel::<i32>(&wal)?;
        let d = rx.recv()?;
        assert_eq!(d.into_inner(), 2);
    }

    Ok(())
}

/* ---------- concurrency ---------- */

#[test]
fn multiple_senders_concurrent() -> anyhow::Result<()> {
    let wal = tmp_wal("multi_sender");
    let (tx, rx) = mpsc::channel::<i32>(&wal)?;

    let t1 = {
        let tx = tx.clone();
        thread::spawn(move || {
            for i in 0..50 {
                tx.send(i).unwrap();
            }
        })
    };

    let t2 = {
        let tx = tx.clone();
        thread::spawn(move || {
            for i in 50..100 {
                tx.send(i).unwrap();
            }
        })
    };

    drop(tx);
    t1.join().unwrap();
    t2.join().unwrap();

    let mut received = Vec::new();
    for d in rx {
        received.push(d.into_inner());
    }

    received.sort();
    assert_eq!(received.len(), 100);
    assert_eq!(received[0], 0);
    assert_eq!(received[99], 99);
    Ok(())
}

#[test]
fn recv_blocks_until_message_arrives() -> anyhow::Result<()> {
    let wal = tmp_wal("blocking");
    let (tx, rx) = mpsc::channel::<i32>(&wal)?;

    let h = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        tx.send(42).unwrap();
    });

    let d = rx.recv()?;
    assert_eq!(d.into_inner(), 42);
    h.join().unwrap();
    Ok(())
}

/* ---------- options & durability ---------- */

#[test]
fn channel_with_options_flush_only_works() -> anyhow::Result<()> {
    let wal = tmp_wal("opt_flush_only");
    let opt = mpsc::Options {
        durability: mpsc::Durability::FlushOnly,
        truncate_corrupt_tail: true,
    };

    let (tx, rx) = mpsc::channel_with_options::<i32>(&wal, opt)?;
    tx.send(10)?;
    tx.send(20)?;
    assert_eq!(rx.recv()?.into_inner(), 10);
    assert_eq!(rx.recv()?.into_inner(), 20);
    Ok(())
}

#[test]
fn channel_with_options_sync_every_works() -> anyhow::Result<()> {
    let wal = tmp_wal("opt_sync_every");
    let opt = mpsc::Options {
        durability: mpsc::Durability::SyncEvery {
            max_ops: 8,
            max_delay: Duration::from_millis(50),
        },
        truncate_corrupt_tail: true,
    };

    let (tx, rx) = mpsc::channel_with_options::<String>(&wal, opt)?;
    for i in 0..20 {
        tx.send(format!("m{i}"))?;
    }

    assert_eq!(rx.recv()?.into_inner(), "m0");
    assert_eq!(rx.recv()?.into_inner(), "m1");
    assert_eq!(rx.recv()?.into_inner(), "m2");
    Ok(())
}

/* ---------- recv_timeout semantics ---------- */

#[test]
fn recv_timeout_times_out() -> anyhow::Result<()> {
    let wal = tmp_wal("timeout");
    let (_tx, rx) = mpsc::channel::<i32>(&wal)?;

    let t0 = std::time::Instant::now();
    let r = rx.recv_timeout(Duration::from_millis(30));
    let elapsed = t0.elapsed();

    assert_eq!(r, Err(mpsc::RecvTimeoutError::Timeout));
    // Allow some scheduler jitter.
    assert!(elapsed >= Duration::from_millis(20));
    Ok(())
}

#[test]
fn recv_timeout_is_woken_by_send() -> anyhow::Result<()> {
    let wal = tmp_wal("timeout_wake");
    let (tx, rx) = mpsc::channel::<i32>(&wal)?;

    let h = thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        tx.send(7).unwrap();
    });

    let d = rx.recv_timeout(Duration::from_millis(300))?;
    assert_eq!(d.into_inner(), 7);
    h.join().unwrap();
    Ok(())
}

/* ---------- WAL tail recovery (truncate_corrupt_tail) ---------- */

#[test]
fn truncate_partial_tail_record_on_open() -> anyhow::Result<()> {
    let wal = tmp_wal("truncate_tail");

    // Write two messages and ack+commit the first one.
    {
        let (tx, rx) = mpsc::channel::<String>(&wal)?;
        tx.send("a".into())?;
        tx.send("b".into())?;
        drop(tx);

        let d = rx.recv()?;
        assert_eq!(d.into_inner(), "a");
        drop(rx); // force commit
    }

    // Append garbage bytes to simulate a torn write / partial record.
    use std::io::{Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new().write(true).open(&wal)?;
    f.seek(SeekFrom::End(0))?;
    f.write_all(&[0xAA, 0xBB, 0xCC, 0xDD, 0xEE])?;
    f.flush()?;
    f.sync_data()?;

    // Reopen: should truncate tail and still read remaining valid record ("b").
    let (_tx2, rx2) = mpsc::channel::<String>(&wal)?;
    let d2 = rx2.recv()?;
    assert_eq!(d2.into_inner(), "b");
    Ok(())
}

#[test]
fn disable_truncate_corrupt_tail_should_error_on_open() -> anyhow::Result<()> {
    let wal = tmp_wal("no_truncate");

    {
        let (tx, _rx) = mpsc::channel::<String>(&wal)?;
        tx.send("a".into())?;
    }

    // Corrupt the tail by appending a fake record header with bad magic.
    use std::io::{Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new().write(true).open(&wal)?;
    f.seek(SeekFrom::End(0))?;
    f.write_all(&0xDEADBEEFu32.to_le_bytes())?;
    f.flush()?;
    f.sync_data()?;

    let opt = mpsc::Options {
        durability: mpsc::Durability::FlushOnly,
        truncate_corrupt_tail: false,
    };

    let r = mpsc::channel_with_options::<String>(&wal, opt);
    assert!(r.is_err());
    Ok(())
}

/* ---------- receiver options (commit batching) ---------- */

#[test]
fn receiver_commit_batching_keeps_order() -> anyhow::Result<()> {
    let wal = tmp_wal("batch_order");
    let (tx, rx) = mpsc::channel::<i32>(&wal)?;
    let rx = rx.with_options(mpsc::ReceiverOptions {
        commit_every: 10,
        commit_max_delay: Duration::ZERO,
    });

    for i in 0..50 {
        tx.send(i)?;
    }
    drop(tx);

    for i in 0..50 {
        let d = rx.recv()?;
        assert_eq!(d.into_inner(), i);
    }
    Ok(())
}

#[test]
fn crash_redeliver_is_bounded_by_commit_window() -> anyhow::Result<()> {
    let wal = tmp_wal("batch_crash_window");

    let (tx, rx) = mpsc::channel::<i32>(&wal)?;
    let rx = rx.with_options(mpsc::ReceiverOptions {
        commit_every: 5,
        commit_max_delay: Duration::ZERO,
    });

    for i in 0..10 {
        tx.send(i)?;
    }
    drop(tx);

    // Consume 6 deliveries, but do NOT drop receiver (simulate crash).
    let mut consumed = Vec::new();
    for _ in 0..6 {
        consumed.push(rx.recv()?.into_inner());
    }
    assert_eq!(consumed, vec![0, 1, 2, 3, 4, 5]);

    std::mem::forget(rx);

    // Reopen and read all remaining messages.
    let (tx2, rx2) = mpsc::channel::<i32>(&wal)?;
    drop(tx2); // critical: otherwise receiver will wait forever at EOF

    let mut after = Vec::new();
    for d in rx2 {
        after.push(d.into_inner());
    }

    assert!(
        after.contains(&9),
        "tail message must be reachable after restart"
    );

    let redelivered = after.iter().filter(|v| **v <= 5).count();
    assert!(
        redelivered <= 6,
        "redelivery window too large: {}",
        redelivered
    );

    Ok(())
}

#[test]
fn receiver_drop_commits_pending_acks() -> anyhow::Result<()> {
    let wal = tmp_wal("batch_drop_force");

    {
        let (tx, rx) = mpsc::channel::<i32>(&wal)?;
        let rx = rx.with_options(mpsc::ReceiverOptions {
            commit_every: 100_000,
            commit_max_delay: Duration::ZERO,
        });

        tx.send(1)?;
        tx.send(2)?;
        drop(tx);

        let d = rx.recv()?;
        assert_eq!(d.into_inner(), 1);

        drop(rx); // must force commit of "1"
    }

    let (_tx2, rx2) = mpsc::channel::<i32>(&wal)?;
    assert_eq!(rx2.recv()?.into_inner(), 2);
    Ok(())
}

/* ---------- bounded (sync_channel v1) ---------- */

#[test]
fn sync_channel_try_send_full_and_ack_frees_capacity() -> anyhow::Result<()> {
    let wal = tmp_wal("sync_try_send");
    let (tx, rx) = mpsc::sync_channel::<i32>(&wal, 1)?;

    // capacity=1: first ok, second should be Full until ack.
    tx.try_send(1)?;
    assert!(matches!(tx.try_send(2), Err(mpsc::TrySendError::Full(2))));

    // Receive and drop delivery => ack => frees capacity.
    let d = rx.recv()?;
    assert_eq!(d.into_inner(), 1);

    tx.try_send(2)?;
    let d2 = rx.recv()?;
    assert_eq!(d2.into_inner(), 2);

    Ok(())
}

#[test]
fn sync_send_timeout_returns_timeout_and_payload() -> anyhow::Result<()> {
    let wal = tmp_wal("send_timeout_full");
    let (tx, _rx) = mpsc::sync_channel::<i32>(&wal, 1)?;

    // 占满容量
    tx.try_send(1)?;

    let t0 = std::time::Instant::now();
    let r = tx.send_timeout(2, Duration::from_millis(30));
    let elapsed = t0.elapsed();

    assert!(matches!(r, Err(mpsc::SendTimeoutError::Timeout(2))));
    assert!(elapsed >= Duration::from_millis(20));

    Ok(())
}

#[test]
fn sync_send_timeout_succeeds_after_ack() -> anyhow::Result<()> {
    let wal = tmp_wal("send_timeout_wait_ack");
    let (tx, rx) = mpsc::sync_channel::<i32>(&wal, 1)?;

    tx.try_send(1)?;

    let rx_hold = rx; // 持有 receiver，不让它 drop

    let h = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(30));
        let d = rx_hold.recv().unwrap();
        assert_eq!(d.into_inner(), 1);
        rx_hold
    });

    tx.send_timeout(2, Duration::from_millis(300))?;

    let rx = h.join().unwrap();
    drop(rx);

    Ok(())
}

#[test]
fn header_slot_corruption_is_tolerated() -> anyhow::Result<()> {
    let wal = tmp_wal("header_slot_corrupt");

    {
        let (tx, rx) = mpsc::channel::<i32>(&wal)?;
        tx.send(1)?;
        drop(tx);

        let d = rx.recv()?;
        assert_eq!(d.into_inner(), 1);
        drop(rx); // force commit
    }

    use std::io::{Seek, SeekFrom, Write};
    let mut f = OpenOptions::new().read(true).write(true).open(&wal)?;
    const SLOT0_OFF: u64 = 0x0C;
    f.seek(SeekFrom::Start(SLOT0_OFF + 16))?; // crc32 offset
    f.write_all(&0xDEADBEEFu32.to_le_bytes())?;
    f.flush()?;
    f.sync_data()?;

    let (tx2, rx2) = mpsc::channel::<i32>(&wal)?;
    drop(tx2);
    assert!(rx2.recv().is_err());

    Ok(())
}

#[test]
fn sync_send_timeout_woken_by_receiver_drop() -> anyhow::Result<()> {
    let wal = tmp_wal("send_timeout_receiver_drop");
    let (tx, rx) = mpsc::sync_channel::<i32>(&wal, 1)?;

    tx.try_send(1)?;

    let h = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(30));
        drop(rx); // receiver gone
    });

    let r = tx.send_timeout(2, Duration::from_millis(300));
    assert!(matches!(r, Err(mpsc::SendTimeoutError::Disconnected(2))));

    h.join().unwrap();
    Ok(())
}

#[test]
fn sync_send_blocks_until_ack() -> anyhow::Result<()> {
    let wal = tmp_wal("sync_send_blocks");
    let (tx, rx) = mpsc::sync_channel::<i32>(&wal, 1)?;

    tx.send(1)?;

    use std::sync::atomic::{AtomicBool, Ordering};
    let finished = Arc::new(AtomicBool::new(false));
    let finished_c = finished.clone();

    let h = std::thread::spawn(move || {
        tx.send(2).unwrap(); // 应阻塞直到 ack
        finished_c.store(true, Ordering::SeqCst);
    });

    // 给 send 一点时间，确保它真的被阻塞
    std::thread::sleep(Duration::from_millis(50));
    assert!(!finished.load(Ordering::SeqCst));

    // recv + drop => ack => 释放 inflight
    let d = rx.recv()?;
    assert_eq!(d.into_inner(), 1);

    h.join().unwrap();
    assert!(finished.load(Ordering::SeqCst));

    Ok(())
}

#[test]
fn commit_max_delay_only_checked_on_ack() -> anyhow::Result<()> {
    let wal = tmp_wal("commit_max_delay_semantics");

    let (tx, rx) = mpsc::channel::<i32>(&wal)?;
    let rx = rx.with_options(mpsc::ReceiverOptions {
        commit_every: 1000,
        commit_max_delay: Duration::from_millis(30),
    });

    tx.send(1)?;
    tx.send(2)?;
    drop(tx);

    let d = rx.recv()?;
    assert_eq!(d.into_inner(), 1);

    std::thread::sleep(Duration::from_millis(80));

    std::mem::forget(rx);

    let (_tx2, rx2) = mpsc::channel::<i32>(&wal)?;
    let v = rx2.recv()?.into_inner();
    assert_eq!(v, 1);

    Ok(())
}

/* ---------- stress tests (enabled by feature) ---------- */

#[cfg(feature = "stress")]
#[test]
fn stress_many_messages_multi_producer_single_consumer() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn run_case(name: &str, durability: mpsc::Durability) -> anyhow::Result<()> {
        let wal = tmp_wal(name);
        let opt = mpsc::Options {
            durability,
            truncate_corrupt_tail: true,
        };

        let (tx, rx) = mpsc::channel_with_options::<u64>(&wal, opt)?;

        let producers = 4usize;
        let per_producer = 50_000usize;
        let total = producers * per_producer;

        let sent = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(producers);

        for p in 0..producers {
            let txc = tx.clone();
            let sentc = sent.clone();
            handles.push(thread::spawn(move || {
                let base = (p as u64) << 48;
                for i in 0..per_producer {
                    txc.send(base | (i as u64)).unwrap();
                    sentc.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        drop(tx);

        let t0 = std::time::Instant::now();
        let mut recv_cnt = 0usize;

        for d in rx {
            let _v = d.into_inner(); // Delivery<T> -> T (RAII ack stays correct)
            recv_cnt += 1;
            if recv_cnt == total {
                break;
            }
        }

        let elapsed = t0.elapsed();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(sent.load(Ordering::Relaxed), total);
        assert_eq!(recv_cnt, total);

        eprintln!(
            "stress[{name}]: total={total}, elapsed={elapsed:?}, msg/s={:.0}",
            total as f64 / elapsed.as_secs_f64()
        );

        Ok(())
    }

    // Small matrix to see durability impact.
    run_case("stress_sync_per_send", mpsc::Durability::SyncPerSend)?;
    run_case("stress_flush_only", mpsc::Durability::FlushOnly)?;
    run_case(
        "stress_sync_every_256_10ms",
        mpsc::Durability::SyncEvery {
            max_ops: 256,
            max_delay: Duration::from_millis(10),
        },
    )?;

    Ok(())
}

/* ---------- stress tests for receiver options (enabled by feature) ---------- */

#[cfg(feature = "stress")]
#[test]
fn stress_receiver_commit_batching_options() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn run_case(name: &str, ro: mpsc::ReceiverOptions) -> anyhow::Result<()> {
        let wal = tmp_wal(name);

        // Keep sender-side consistent so we measure receiver-side batching.
        let opt = mpsc::Options {
            durability: mpsc::Durability::SyncEvery {
                max_ops: 256,
                max_delay: Duration::from_millis(10),
            },
            truncate_corrupt_tail: true,
        };

        let commit_every = ro.commit_every;
        let commit_max_delay = ro.commit_max_delay;

        let (tx, rx) = mpsc::channel_with_options::<u64>(&wal, opt)?;
        let rx = rx.with_options(ro);

        let producers = 4usize;
        let per_producer = 50_000usize;
        let total = producers * per_producer;

        let sent = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(producers);

        for p in 0..producers {
            let txc = tx.clone();
            let sentc = sent.clone();
            handles.push(thread::spawn(move || {
                let base = (p as u64) << 48;
                for i in 0..per_producer {
                    txc.send(base | (i as u64)).unwrap();
                    sentc.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        drop(tx);

        let t0 = std::time::Instant::now();
        let mut recv_cnt = 0usize;

        for d in rx {
            let _v = d.into_inner();
            recv_cnt += 1;
            if recv_cnt == total {
                break;
            }
        }

        let elapsed = t0.elapsed();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(sent.load(Ordering::Relaxed), total);
        assert_eq!(recv_cnt, total);

        eprintln!(
            "stress_rxopt[{name}]: total={total}, elapsed={elapsed:?}, msg/s={:.0}, commit_every={}, commit_max_delay={:?}",
            total as f64 / elapsed.as_secs_f64(),
            commit_every,
            commit_max_delay
        );

        Ok(())
    }

    // Baseline: commit every message.
    run_case(
        "rxopt_commit_every_1",
        mpsc::ReceiverOptions {
            commit_every: 1,
            commit_max_delay: Duration::ZERO,
        },
    )?;

    // Batch by count only.
    run_case(
        "rxopt_commit_every_32",
        mpsc::ReceiverOptions {
            commit_every: 32,
            commit_max_delay: Duration::ZERO,
        },
    )?;
    run_case(
        "rxopt_commit_every_128",
        mpsc::ReceiverOptions {
            commit_every: 128,
            commit_max_delay: Duration::ZERO,
        },
    )?;

    // Batch by count + time (bursty traffic).
    run_case(
        "rxopt_128_or_10ms",
        mpsc::ReceiverOptions {
            commit_every: 128,
            commit_max_delay: Duration::from_millis(10),
        },
    )?;

    Ok(())
}
