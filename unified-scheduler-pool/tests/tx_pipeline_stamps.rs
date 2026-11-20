#![cfg(feature = "agave-unstable-api")]
//! Checks that a transaction scheduled through the unified scheduler gets the tx_pipeline
//! scheduler stamps (`mark_tx_scheduled`, `mark_tx_exec_started`, `mark_tx_executed`).
//!
//! Kept in its own test binary (one test, one process): it reads deltas of global histograms,
//! and running it alongside the crate's `sleepless_testing` unit tests shifted their thread
//! timing enough to make `test_scheduler_schedule_execution_failure_*` hang (their pool cleaner
//! must tick once before the scheduler is trashed). Don't add more tests to this file without a
//! lock around the global histograms.

use {
    assert_matches::assert_matches,
    solana_metrics::{
        pipeline_latency::TX_PIPELINE_LATENCY,
        pipeline_metrics::{SCHEDULER_QUEUE_WAIT_DURATION_US, TX_PIPELINE_SEGMENT_DURATION_US},
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
    solana_unified_scheduler_pool::DefaultSchedulerPool,
    std::{sync::Arc, time::Instant},
};

// `signature()` is on a supertrait of `TransactionWithMeta`; a generic bound reaches it.
fn signature_bytes(transaction: &impl TransactionWithMeta) -> [u8; 64] {
    (*transaction.signature()).into()
}

#[test]
fn test_scheduler_schedule_execution_stamps_tx_pipeline() {
    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(10_000);
    let tx0 = ReplayTransaction::from(solana_system_transaction::transfer(
        &mint_keypair,
        &solana_pubkey::new_rand(),
        2,
        genesis_config.hash(),
    ));
    let signature = signature_bytes(&tx0);
    const SLOT: u64 = 7;
    TX_PIPELINE_LATENCY.mark_data_set_started(SLOT, 0);
    TX_PIPELINE_LATENCY.mark_data_set_ready(SLOT, 0);
    TX_PIPELINE_LATENCY.mark_tx_data_set(signature, SLOT, 0);

    let segment_count = |name: &str| {
        TX_PIPELINE_SEGMENT_DURATION_US
            .with_label_values(&[name])
            .get_sample_count()
    };
    let wait_count = || {
        ["blocked", "unblocked"].map(|task| {
            SCHEDULER_QUEUE_WAIT_DURATION_US
                .with_label_values(&[task])
                .get_sample_count()
        })
    };
    let split = [
        "replay_pickup_to_scheduled",
        "scheduled_to_exec_start",
        "exec_start_to_executed",
        "executed_to_committed",
    ];
    let split_before = split.map(segment_count);
    let whole_before = segment_count("replay_pickup_to_committed");
    let wait_before = wait_count();

    let bank = Bank::new_for_tests(&genesis_config);
    let slot = bank.slot();
    let bank_forks = BankForks::new_rw_arc(bank);
    let bank = bank_forks.read().unwrap().get(slot).unwrap();
    bank.set_fork_graph_in_program_cache(Arc::downgrade(&bank_forks));
    let pool = DefaultSchedulerPool::new_for_verification(None, None, None, None, None);
    let context = SchedulingContext::new(bank.clone());
    let scheduler = pool.take_scheduler(context).unwrap();
    scheduler.schedule_execution(tx0, 0).unwrap();
    let bank = BankWithScheduler::new(bank, Some(scheduler));
    assert_matches!(bank.wait_for_completed_scheduler(), Some((Ok(()), _)));

    // A single, non-conflicting task is never blocked.
    let wait_after = wait_count();
    assert_eq!(wait_after[0], wait_before[0]);
    assert_eq!(wait_after[1] - wait_before[1], 1);

    // No tx-status sender in this pool; stand in for the tx-status service and notifier.
    TX_PIPELINE_LATENCY.mark_tx_status_received(signature, Instant::now());
    TX_PIPELINE_LATENCY.mark_tx_notified(signature);
    for (name, before) in split.iter().zip(split_before) {
        assert_eq!(segment_count(name) - before, 1, "{name}");
    }
    assert_eq!(segment_count("replay_pickup_to_committed"), whole_before);
}
