use {
    crate::bank::Bank,
    solana_clock::{Epoch, Slot},
    solana_program_runtime::program_metrics::ProgramCacheStats,
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        sync::{
            LazyLock, Mutex, PoisonError,
            atomic::{
                AtomicU64,
                Ordering::{self, Relaxed},
            },
        },
    },
};

pub(crate) struct NewEpochTimings {
    pub(crate) apply_feature_activations_time_us: u64,
    pub(crate) calculate_activated_stake_time_us: u64,
    pub(crate) update_epoch_stakes_time_us: u64,
    pub(crate) update_rewards_with_thread_pool_time_us: u64,
    pub(crate) begin_partitioned_rewards_time_us: u64,
}

#[derive(Debug, Default)]
pub(crate) struct RewardsMetrics {
    pub(crate) calculate_points_us: AtomicU64,
    pub(crate) redeem_rewards_us: u64,
    pub(crate) store_stake_accounts_us: AtomicU64,
    pub(crate) store_epoch_boundary_accounts_us: AtomicU64,
    pub(crate) load_and_update_epoch_boundary_accounts_us: u64,
}

pub(crate) struct NewBankTimings {
    pub(crate) bank_rc_creation_time_us: u64,
    pub(crate) total_elapsed_time_us: u64,
    pub(crate) status_cache_time_us: u64,
    pub(crate) fee_components_time_us: u64,
    pub(crate) blockhash_queue_time_us: u64,
    pub(crate) stakes_cache_time_us: u64,
    pub(crate) epoch_stakes_time_us: u64,
    pub(crate) builtin_program_ids_time_us: u64,
    pub(crate) executor_cache_time_us: u64,
    pub(crate) transaction_debug_keys_time_us: u64,
    pub(crate) transaction_log_collector_config_time_us: u64,
    pub(crate) feature_set_time_us: u64,
    pub(crate) ancestors_time_us: u64,
    pub(crate) update_epoch_time_us: u64,
    pub(crate) distribute_rewards_time_us: u64,
    pub(crate) cache_preparation_time_us: u64,
    pub(crate) update_sysvars_time_us: u64,
    pub(crate) fill_sysvar_cache_time_us: u64,
}

pub(crate) struct PrepareBlockExecutionStats {
    pub(crate) update_epoch_time_us: u64,
    pub(crate) distribute_rewards_time_us: u64,
    pub(crate) cache_preparation_time_us: u64,
    pub(crate) update_sysvars_time_us: u64,
    pub(crate) fill_sysvar_cache_time_us: u64,
}

pub(crate) fn report_new_epoch_metrics(
    epoch: Epoch,
    slot: Slot,
    parent_slot: Slot,
    timings: NewEpochTimings,
    metrics: RewardsMetrics,
) {
    datapoint_info!(
        "bank-new_from_parent-new_epoch_timings",
        ("epoch", epoch, i64),
        ("slot", slot, i64),
        ("parent_slot", parent_slot, i64),
        (
            "apply_feature_activations",
            timings.apply_feature_activations_time_us,
            i64
        ),
        (
            "calculate_activated_stake_us",
            timings.calculate_activated_stake_time_us,
            i64
        ),
        (
            "update_epoch_stakes_us",
            timings.update_epoch_stakes_time_us,
            i64
        ),
        (
            "update_rewards_with_thread_pool_us",
            timings.update_rewards_with_thread_pool_time_us,
            i64
        ),
        (
            "begin_partitioned_rewards_us",
            timings.begin_partitioned_rewards_time_us,
            i64
        ),
        (
            "calculate_points_us",
            metrics.calculate_points_us.load(Relaxed),
            i64
        ),
        ("redeem_rewards_us", metrics.redeem_rewards_us, i64),
        (
            "store_stake_accounts_us",
            metrics.store_stake_accounts_us.load(Relaxed),
            i64
        ),
        (
            "store_epoch_boundary_accounts_us",
            metrics.store_epoch_boundary_accounts_us.load(Relaxed),
            i64
        ),
        (
            "load_and_update_epoch_boundary_accounts_us",
            metrics.load_and_update_epoch_boundary_accounts_us,
            i64
        ),
    );
}

pub(crate) fn report_new_bank_metrics(
    slot: Slot,
    parent_slot: Slot,
    block_height: u64,
    timings: NewBankTimings,
) {
    datapoint_info!(
        "bank-new_from_parent-heights",
        ("slot", slot, i64),
        ("block_height", block_height, i64),
        ("parent_slot", parent_slot, i64),
        ("bank_rc_creation_us", timings.bank_rc_creation_time_us, i64),
        ("total_elapsed_us", timings.total_elapsed_time_us, i64),
        ("status_cache_us", timings.status_cache_time_us, i64),
        ("fee_components_us", timings.fee_components_time_us, i64),
        ("blockhash_queue_us", timings.blockhash_queue_time_us, i64),
        ("stakes_cache_us", timings.stakes_cache_time_us, i64),
        ("epoch_stakes_time_us", timings.epoch_stakes_time_us, i64),
        (
            "builtin_programs_us",
            timings.builtin_program_ids_time_us,
            i64
        ),
        ("executor_cache_us", timings.executor_cache_time_us, i64),
        (
            "transaction_debug_keys_us",
            timings.transaction_debug_keys_time_us,
            i64
        ),
        (
            "transaction_log_collector_config_us",
            timings.transaction_log_collector_config_time_us,
            i64
        ),
        ("feature_set_us", timings.feature_set_time_us, i64),
        ("ancestors_us", timings.ancestors_time_us, i64),
        ("update_epoch_us", timings.update_epoch_time_us, i64),
        (
            "distribute_rewards_us",
            timings.distribute_rewards_time_us,
            i64
        ),
        (
            "cache_preparation_time_us",
            timings.cache_preparation_time_us,
            i64
        ),
        ("update_sysvars_us", timings.update_sysvars_time_us, i64),
        (
            "fill_sysvar_cache_us",
            timings.fill_sysvar_cache_time_us,
            i64
        ),
    );
}

/// Metrics for partitioned epoch reward store
#[derive(Debug, Default)]
pub(crate) struct RewardsStoreMetrics {
    pub(crate) total_num_partitions: usize,
    pub(crate) partition_index: u64,
    pub(crate) store_stake_accounts_us: u64,
    pub(crate) store_stake_accounts_count: usize,
    pub(crate) total_stake_accounts_count: usize,
    pub(crate) distributed_rewards: u64,
    pub(crate) burned_rewards: u64,
    pub(crate) distributed_block_rewards: u64,
    pub(crate) burned_block_rewards: u64,
    pub(crate) pre_capitalization: u64,
    pub(crate) post_capitalization: u64,
}

pub(crate) fn report_partitioned_reward_metrics(bank: &Bank, timings: RewardsStoreMetrics) {
    datapoint_info!(
        "bank-partitioned_epoch_rewards_credit",
        ("slot", bank.slot(), i64),
        ("epoch", bank.epoch(), i64),
        ("block_height", bank.block_height(), i64),
        ("parent_slot", bank.parent_slot(), i64),
        ("partition_index", timings.partition_index, i64),
        (
            "store_stake_accounts_us",
            timings.store_stake_accounts_us,
            i64
        ),
        ("total_num_partitions", timings.total_num_partitions, i64),
        (
            "store_stake_accounts_count",
            timings.store_stake_accounts_count,
            i64
        ),
        (
            "total_stake_accounts_count",
            timings.total_stake_accounts_count,
            i64
        ),
        ("distributed_rewards", timings.distributed_rewards, i64),
        ("burned_rewards", timings.burned_rewards, i64),
        (
            "distributed_block_rewards",
            timings.distributed_block_rewards,
            i64
        ),
        ("burned_block_rewards", timings.burned_block_rewards, i64),
        ("pre_capitalization", timings.pre_capitalization, i64),
        ("post_capitalization", timings.post_capitalization, i64),
    );
}

/// Slots between two logs of the most evicted programs.
const PROGRAM_EVICTIONS_LOG_INTERVAL_SLOTS: Slot = 1000;
/// Programs listed in that log.
const PROGRAM_EVICTIONS_LOG_TOP: usize = 20;
/// Slots between two samples of the process's resident memory.
const RESIDENT_MEMORY_SAMPLE_INTERVAL_SLOTS: Slot = 64;

/// Evictions per program, accumulated over a log window.
type ProgramEvictions = HashMap<Pubkey, u64>;

/// Program cache reporting state kept across bank creations.
#[derive(Debug, Default)]
struct ProgramCacheReportState {
    /// Evictions per program since the last eviction log.
    evictions: ProgramEvictions,
    /// Slot the current eviction log window started at.
    evictions_window_start: Option<Slot>,
    /// Slot of the last resident memory sample.
    last_resident_memory_sample: Option<Slot>,
}

impl ProgramCacheReportState {
    /// Adds one report's evictions (`slot` being the reported bank's slot).
    /// Returns the evictions to log with the window length in slots, once a
    /// log window has elapsed and something was evicted in it, and whether to
    /// sample resident memory now.
    fn record(
        &mut self,
        slot: Slot,
        evictions: &HashMap<Pubkey, u64>,
    ) -> (Option<(Slot, ProgramEvictions)>, bool) {
        for (program, count) in evictions {
            let total = self.evictions.entry(*program).or_default();
            *total = total.saturating_add(*count);
        }
        let window_start = self.evictions_window_start;
        let evictions_to_log = window_elapsed(
            &mut self.evictions_window_start,
            slot,
            PROGRAM_EVICTIONS_LOG_INTERVAL_SLOTS,
        )
        .then(|| {
            let window_slots = slot.saturating_sub(window_start.unwrap_or(slot));
            (window_slots, std::mem::take(&mut self.evictions))
        })
        .filter(|(_, evictions)| !evictions.is_empty());
        let sample_resident_memory = window_elapsed(
            &mut self.last_resident_memory_sample,
            slot,
            RESIDENT_MEMORY_SAMPLE_INTERVAL_SLOTS,
        );
        (evictions_to_log, sample_resident_memory)
    }
}

static PROGRAM_CACHE_REPORT_STATE: LazyLock<Mutex<ProgramCacheReportState>> =
    LazyLock::new(Mutex::default);

/// Whether at least `interval` slots passed since `*last`, in which case
/// `*last` moves to `slot`. The first call only sets `*last`. A `slot` below
/// `*last` (reports can go backwards across forks) leaves it unchanged, and a
/// window never stretches past `interval` because the slot that would end it
/// was skipped.
fn window_elapsed(last: &mut Option<Slot>, slot: Slot, interval: Slot) -> bool {
    match *last {
        Some(last_slot) => {
            let elapsed = slot >= last_slot && slot.saturating_sub(last_slot) >= interval;
            if elapsed {
                *last = Some(slot);
            }
            elapsed
        }
        None => {
            *last = Some(slot);
            false
        }
    }
}

/// The `top` most evicted programs, most evicted first (ties by address).
fn top_evicted(evictions: HashMap<Pubkey, u64>, top: usize) -> Vec<(Pubkey, u64)> {
    let mut evictions: Vec<_> = evictions.into_iter().collect();
    evictions.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    evictions.truncate(top);
    evictions
}

/// Resident set size of this process in bytes, or `None` where unavailable.
fn resident_memory_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let resident_pages = parse_statm_resident_pages(&statm)?;
        // SAFETY: `sysconf` has no preconditions; it only reads a system
        // configuration value.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let page_size = u64::try_from(page_size).ok().filter(|&size| size > 0)?;
        Some(resident_pages.saturating_mul(page_size))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// The resident page count (second field) of a `/proc/<pid>/statm` line.
#[cfg(any(target_os = "linux", test))]
fn parse_statm_resident_pages(statm: &str) -> Option<u64> {
    statm.split_whitespace().nth(1)?.parse().ok()
}

/// Logs the measurement values and exports them to Prometheus. `stats` were
/// taken from (and reset in) the global program cache when a bank was created
/// from the bank at `slot`, so they cover everything since the previous bank
/// creation. Called without the program cache lock held.
pub(crate) fn report_loaded_programs_stats(stats: &ProgramCacheStats, slot: Slot) {
    let hits = stats.hits.load(Ordering::Relaxed);
    let misses = stats.misses.load(Ordering::Relaxed);
    let evictions: u64 = stats.evictions.values().sum();
    let reloads = stats.reloads.load(Ordering::Relaxed);
    let insertions = stats.insertions.load(Ordering::Relaxed);
    let lost_insertions = stats.lost_insertions.load(Ordering::Relaxed);
    let replacements = stats.replacements.load(Ordering::Relaxed);
    let one_hit_wonders = stats.one_hit_wonders.load(Ordering::Relaxed);
    let prunes_stale = stats.prunes_stale.load(Ordering::Relaxed);
    let prunes_orphan = stats.prunes_orphan.load(Ordering::Relaxed);
    let prunes_environment = stats.prunes_environment.load(Ordering::Relaxed);
    let empty_entries = stats.empty_entries.load(Ordering::Relaxed);
    let water_level = stats.water_level.load(Ordering::Relaxed);
    datapoint_info!(
        "loaded-programs-cache-stats",
        ("slot", slot, i64),
        ("hits", hits, i64),
        ("misses", misses, i64),
        ("evictions", evictions, i64),
        ("reloads", reloads, i64),
        ("insertions", insertions, i64),
        ("lost_insertions", lost_insertions, i64),
        ("replace_entry", replacements, i64),
        ("one_hit_wonders", one_hit_wonders, i64),
        ("prunes_stale", prunes_stale, i64),
        ("prunes_orphan", prunes_orphan, i64),
        ("prunes_environment", prunes_environment, i64),
        ("empty_entries", empty_entries, i64),
        ("water_level", water_level, i64),
    );
    stats.log();

    let events = &solana_metrics::pipeline_metrics::PROGRAM_CACHE_EVENTS_TOTAL;
    for (event, count) in [
        ("hit", hits),
        ("miss", misses),
        ("eviction", evictions),
        ("reload", reloads),
        ("insertion", insertions),
        ("lost_insertion", lost_insertions),
        ("replacement", replacements),
        ("one_hit_wonder", one_hit_wonders),
        ("prune_stale", prunes_stale),
        ("prune_orphan", prunes_orphan),
        ("prune_environment", prunes_environment),
        ("empty_entry", empty_entries),
    ] {
        events.with_label_values(&[event]).inc_by(count);
    }
    if water_level > 0 {
        solana_metrics::pipeline_metrics::PROGRAM_CACHE_LOADED_ENTRIES.set(water_level as f64);
    }

    let (evictions_to_log, sample_resident_memory) = PROGRAM_CACHE_REPORT_STATE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .record(slot, &stats.evictions);
    if let Some((window_slots, evicted)) = evictions_to_log {
        let total: u64 = evicted.values().sum();
        let num_programs = evicted.len();
        let most_evicted = top_evicted(evicted, PROGRAM_EVICTIONS_LOG_TOP)
            .into_iter()
            .map(|(program, count)| format!("{program}:{count}"))
            .collect::<Vec<_>>()
            .join(", ");
        log::info!(
            "program cache evictions over the last {window_slots} slots: {total} evictions of \
             {num_programs} programs; most evicted: {most_evicted}"
        );
    }
    if sample_resident_memory && let Some(bytes) = resident_memory_bytes() {
        solana_metrics::pipeline_metrics::PROCESS_RESIDENT_MEMORY_BYTES.set(bytes as f64);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{bank::SlotLeader, genesis_utils::create_genesis_config},
    };

    fn program_cache_event_count(event: &str) -> u64 {
        solana_metrics::pipeline_metrics::PROGRAM_CACHE_EVENTS_TOTAL
            .with_label_values(&[event])
            .get()
    }

    #[test]
    fn test_window_elapsed() {
        let mut last = None;
        // The first call only anchors the window.
        assert!(!window_elapsed(&mut last, 100, 64));
        assert_eq!(last, Some(100));
        assert!(!window_elapsed(&mut last, 163, 64));
        assert_eq!(last, Some(100));
        assert!(window_elapsed(&mut last, 164, 64));
        assert_eq!(last, Some(164));
        // Skipped slots don't stretch the window: any slot past its end ends it.
        assert!(window_elapsed(&mut last, 300, 64));
        assert_eq!(last, Some(300));
        // Going backwards (another fork) neither fires nor moves the window.
        assert!(!window_elapsed(&mut last, 250, 64));
        assert_eq!(last, Some(300));
        assert!(!window_elapsed(&mut last, 363, 64));
        assert!(window_elapsed(&mut last, 364, 64));
        assert_eq!(last, Some(364));
    }

    #[test]
    fn test_report_state_accumulates_and_logs_evictions() {
        let mut state = ProgramCacheReportState::default();
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        assert_eq!(state.record(10, &HashMap::from([(a, 2)])).0, None);
        assert_eq!(state.record(500, &HashMap::from([(a, 1), (b, 4)])).0, None);

        let (window_slots, evicted) = state.record(1010, &HashMap::new()).0.unwrap();
        assert_eq!(window_slots, 1000);
        assert_eq!(evicted, HashMap::from([(a, 3), (b, 4)]));
        assert!(state.evictions.is_empty());

        // A window with no evictions logs nothing.
        assert_eq!(state.record(2010, &HashMap::new()).0, None);
    }

    #[test]
    fn test_report_state_samples_resident_memory() {
        let mut state = ProgramCacheReportState::default();
        assert!(!state.record(0, &HashMap::new()).1);
        assert!(!state.record(63, &HashMap::new()).1);
        assert!(state.record(64, &HashMap::new()).1);
        assert!(!state.record(65, &HashMap::new()).1);
    }

    #[test]
    fn test_top_evicted() {
        let a = Pubkey::new_from_array([1; 32]);
        let b = Pubkey::new_from_array([2; 32]);
        let c = Pubkey::new_from_array([3; 32]);
        let evictions = HashMap::from([(a, 5), (b, 9), (c, 5)]);
        assert_eq!(
            top_evicted(evictions.clone(), 10),
            vec![(b, 9), (a, 5), (c, 5)]
        );
        assert_eq!(top_evicted(evictions, 2), vec![(b, 9), (a, 5)]);
        assert!(top_evicted(HashMap::new(), 2).is_empty());
    }

    #[test]
    fn test_parse_statm_resident_pages() {
        assert_eq!(
            parse_statm_resident_pages("1000 250 30 4 0 500 0\n"),
            Some(250)
        );
        assert_eq!(parse_statm_resident_pages("1000"), None);
        assert_eq!(parse_statm_resident_pages(""), None);
        assert_eq!(parse_statm_resident_pages("1000 x"), None);
    }

    // The counters are process-wide and other tests create banks concurrently,
    // so only lower bounds on the deltas can be asserted.
    #[test]
    fn test_report_exports_program_cache_counters() {
        let mut stats = ProgramCacheStats::default();
        stats.hits.store(1_000_003, Relaxed);
        stats.misses.store(7, Relaxed);
        stats.reloads.store(5, Relaxed);
        stats.evictions.insert(Pubkey::new_unique(), 11);
        let hits = program_cache_event_count("hit");
        let misses = program_cache_event_count("miss");
        let reloads = program_cache_event_count("reload");
        let evictions = program_cache_event_count("eviction");

        report_loaded_programs_stats(&stats, 1);

        assert!(program_cache_event_count("hit").saturating_sub(hits) >= 1_000_003);
        assert!(program_cache_event_count("miss").saturating_sub(misses) >= 7);
        assert!(program_cache_event_count("reload").saturating_sub(reloads) >= 5);
        assert!(program_cache_event_count("eviction").saturating_sub(evictions) >= 11);
    }

    #[test]
    fn test_new_from_parent_takes_and_resets_program_cache_stats() {
        let genesis_config = create_genesis_config(1_000_000).genesis_config;
        let (parent, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        parent
            .transaction_processor
            .global_program_cache
            .write()
            .unwrap()
            .stats
            .misses
            .store(2_000_017, Relaxed);
        let misses = program_cache_event_count("miss");

        let child =
            Bank::new_from_parent_with_bank_forks(&bank_forks, parent, SlotLeader::default(), 1);

        // The shared cache's statistics were taken and reset...
        assert_eq!(
            child
                .transaction_processor
                .global_program_cache
                .read()
                .unwrap()
                .stats
                .misses
                .load(Relaxed),
            0
        );
        // ...and exported.
        assert!(program_cache_event_count("miss").saturating_sub(misses) >= 2_000_017);
    }
}
