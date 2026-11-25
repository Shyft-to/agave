#![cfg(feature = "agave-unstable-api")]
//! Checks `agave_scheduler_blocked_wait_split_us` end to end: a transaction blocked behind a
//! conflicting one gets its queue wait split at that predecessor, with the predecessor's commit
//! and hand-back stamps carried through the scheduler.
//!
//! Kept in its own test binary (one test, one process): it reads deltas of global histograms.
//! Don't add more tests to this file without a lock around the global histograms.

use {
    assert_matches::assert_matches,
    solana_metrics::{
        pipeline_latency::TX_PIPELINE_LATENCY,
        pipeline_metrics::{SCHEDULER_BLOCKED_WAIT_SPLIT_US, SCHEDULER_QUEUE_WAIT_DURATION_US},
    },
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::{GenesisConfigInfo, create_genesis_config},
        installed_scheduler_pool::{BankWithScheduler, InstalledSchedulerPool, SchedulingContext},
    },
    solana_runtime_transaction::{
        runtime_transaction::ReplayTransaction, transaction_with_meta::TransactionWithMeta,
    },
    solana_svm_timings::ExecuteTimings,
    solana_transaction_error::TransactionResult,
    solana_unified_scheduler_logic::Task,
    solana_unified_scheduler_pool::{
        DefaultTaskHandler, HandlerContext, PooledScheduler, SchedulerPool, TaskHandler,
    },
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::sleep,
        time::Duration,
    },
};

/// How long the first task is held, after the second one has been scheduled, before it executes:
/// the second, conflicting task reaches the scheduler while the first still holds the payer's
/// write lock (it is then blocked), and its wait covers at least this hold.
const HOLD: Duration = Duration::from_millis(50);

/// Set by the test once both transactions have been scheduled (stamped `scheduled`).
static SECOND_SCHEDULED: AtomicBool = AtomicBool::new(false);

#[derive(Debug)]
struct HoldFirstTask;

impl TaskHandler for HoldFirstTask {
    fn handle(
        result: &mut TransactionResult<()>,
        timings: &mut ExecuteTimings,
        scheduling_context: &SchedulingContext,
        task: &Task,
        handler_context: &HandlerContext,
    ) {
        if task.task_id() == 0 {
            while !SECOND_SCHEDULED.load(Ordering::Acquire) {
                sleep(Duration::from_millis(1));
            }
            sleep(HOLD);
        }
        DefaultTaskHandler::handle(result, timings, scheduling_context, task, handler_context);
    }
}

// `signature()` is on a supertrait of `TransactionWithMeta`; a generic bound reaches it.
fn signature_bytes(transaction: &impl TransactionWithMeta) -> [u8; 64] {
    (*transaction.signature()).into()
}

#[test]
fn test_scheduler_blocked_wait_split() {
    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(10_000_000);
    // Both write-lock the mint (payer), so the second is blocked behind the first.
    let txs: Vec<_> = (0..2)
        .map(|_| {
            ReplayTransaction::from(solana_system_transaction::transfer(
                &mint_keypair,
                &solana_pubkey::new_rand(),
                2,
                genesis_config.hash(),
            ))
        })
        .collect();
    const SLOT: u64 = 7;
    TX_PIPELINE_LATENCY.mark_data_set_started(SLOT, 0);
    for tx in &txs {
        TX_PIPELINE_LATENCY.mark_tx_data_set(signature_bytes(tx), SLOT, 0);
    }

    let parts = [
        "predecessor_running",
        "post_commit",
        "return_hop",
        "dispatch",
    ];
    let read_split = || {
        parts.map(|part| {
            let h = SCHEDULER_BLOCKED_WAIT_SPLIT_US.with_label_values(&[part]);
            (h.get_sample_count(), h.get_sample_sum())
        })
    };
    let read_blocked_wait = || {
        let h = SCHEDULER_QUEUE_WAIT_DURATION_US.with_label_values(&["blocked"]);
        (h.get_sample_count(), h.get_sample_sum())
    };
    let split_before = read_split();
    let (wait_count_before, wait_sum_before) = read_blocked_wait();

    let bank = Bank::new_for_tests(&genesis_config);
    let slot = bank.slot();
    let bank_forks = BankForks::new_rw_arc(bank);
    let bank = bank_forks.read().unwrap().get(slot).unwrap();
    bank.set_fork_graph_in_program_cache(Arc::downgrade(&bank_forks));
    let pool = SchedulerPool::<PooledScheduler<HoldFirstTask>, _>::new_for_verification(
        None, None, None, None, None,
    );
    let context = SchedulingContext::new(bank.clone());
    let scheduler = pool.take_scheduler(context).unwrap();
    for (task_id, tx) in txs.into_iter().enumerate() {
        scheduler.schedule_execution(tx, task_id as u128).unwrap();
    }
    SECOND_SCHEDULED.store(true, Ordering::Release);
    let bank = BankWithScheduler::new(bank, Some(scheduler));
    assert_matches!(bank.wait_for_completed_scheduler(), Some((Ok(()), _)));

    // The second transaction was blocked: one queue-wait sample and one split.
    let (wait_count_after, wait_sum_after) = read_blocked_wait();
    assert_eq!(wait_count_after - wait_count_before, 1);
    let split_after = read_split();
    let mut split_total = 0.0;
    for ((part, (count_before, sum_before)), (count_after, sum_after)) in
        parts.iter().zip(split_before).zip(split_after)
    {
        assert_eq!(count_after - count_before, 1, "{part}");
        split_total += sum_after - sum_before;
    }
    // It waited for the held first transaction (held for HOLD after the second was scheduled),
    // which shows up as predecessor_running.
    let predecessor_running = split_after[0].1 - split_before[0].1;
    assert!(
        predecessor_running >= HOLD.as_micros() as f64,
        "predecessor_running {predecessor_running}us"
    );
    // The parts telescope to the blocked queue wait (up to per-part us truncation).
    let wait = wait_sum_after - wait_sum_before;
    assert!(
        (wait - split_total).abs() <= 4.0,
        "split {split_total}us vs wait {wait}us"
    );
}
