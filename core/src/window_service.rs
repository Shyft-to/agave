//! `window_service` handles the data plane incoming shreds, storing them in
//!   blockstore and retransmitting where required
//!

use {
    crate::{
        completed_data_sets_service::CompletedDataSetsSender,
        repair::{
            block_id_repair_service::{BlockIdRepairChannels, BlockIdRepairService},
            repair_service::{
                OutstandingShredRepairs, RepairInfo, RepairService, RepairServiceChannels,
            },
        },
        result::{Error, Result},
    },
    agave_feature_set as feature_set,
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded},
    rayon::{ThreadPool, prelude::*},
    solana_clock::Slot,
    solana_entry::block_component::ParsedBlockComponent,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore::{
            Blockstore, BlockstoreInsertionMetrics, CompletedDataSetInfo, PossibleDuplicateShred,
            handle_duplicate_shred,
        },
        blockstore_db::{DBPinnableSlice, WriteBatch},
        blockstore_meta::BlockLocation,
        live_replay_entries::{
            LiveReplayMessage, LiveReplaySender, PartialDataSetDecoder, PartialDecoderStop,
        },
        shred::{self, ReedSolomonCache, Shred, filter::ShredRecoveryContext},
    },
    solana_measure::measure::Measure,
    solana_net_utils::PinnedXdpSender,
    solana_rayon_threadlimit::get_thread_count,
    solana_runtime::bank_forks::{BankForks, SharableBanks},
    solana_streamer::evicting_sender::EvictingSender,
    std::{
        borrow::Cow,
        collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
        net::UdpSocket,
        ops::Range,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

type DuplicateSlotSender = Sender<Slot>;
pub(crate) type DuplicateSlotReceiver = Receiver<Slot>;

#[derive(Default)]
struct WindowServiceMetrics {
    run_insert_count: u64,
    num_repairs: AtomicUsize,
    num_shreds_received: usize,
    handle_packets_elapsed_us: u64,
    shred_receiver_elapsed_us: u64,
    num_errors: u64,
    num_errors_blockstore: u64,
    num_errors_cross_beam_recv_timeout: u64,
    num_errors_other: u64,
    num_errors_try_crossbeam_send: u64,
}

impl WindowServiceMetrics {
    const NAME: &str = "recv-window-insert-shreds";

    fn report_metrics(&self) {
        datapoint_info!(
            Self::NAME,
            (
                "handle_packets_elapsed_us",
                self.handle_packets_elapsed_us,
                i64
            ),
            ("run_insert_count", self.run_insert_count as i64, i64),
            ("num_repairs", self.num_repairs.load(Ordering::Relaxed), i64),
            ("num_shreds_received", self.num_shreds_received, i64),
            (
                "shred_receiver_elapsed_us",
                self.shred_receiver_elapsed_us as i64,
                i64
            ),
            ("num_errors", self.num_errors, i64),
            ("num_errors_blockstore", self.num_errors_blockstore, i64),
            ("num_errors_other", self.num_errors_other, i64),
            (
                "num_errors_try_crossbeam_send",
                self.num_errors_try_crossbeam_send,
                i64
            ),
            (
                "num_errors_cross_beam_recv_timeout",
                self.num_errors_cross_beam_recv_timeout,
                i64
            ),
        );
    }

    fn record_error(&mut self, err: &Error) {
        self.num_errors += 1;
        match err {
            Error::TrySend => self.num_errors_try_crossbeam_send += 1,
            Error::RecvTimeout(_) => self.num_errors_cross_beam_recv_timeout += 1,
            Error::Blockstore(err) => {
                self.num_errors_blockstore += 1;
                error!("blockstore error: {err}");
            }
            _ => self.num_errors_other += 1,
        }
    }
}

fn run_check_duplicate(
    cluster_info: &ClusterInfo,
    blockstore: &Blockstore,
    shred_receiver: &Receiver<PossibleDuplicateShred>,
    duplicate_slots_sender: &DuplicateSlotSender,
    bank_forks: &RwLock<BankForks>,
) -> Result<()> {
    let (mut root_bank, migration_status) = {
        let bank_forks_r = bank_forks.read().unwrap();
        (bank_forks_r.root_bank(), bank_forks_r.migration_status())
    };
    let mut last_updated = Instant::now();
    let check_duplicate = |shred: PossibleDuplicateShred| -> Result<()> {
        if last_updated.elapsed().as_nanos() > root_bank.ns_per_slot {
            // Grabs bank forks lock once a slot
            last_updated = Instant::now();
            root_bank = bank_forks.read().unwrap().root_bank();
        }
        let shred_slot = shred.slot();
        let no_verify_chained_merkle_root = shred::filter::check_feature_activation_from_bank(
            &feature_set::alpenglow::id(),
            shred_slot,
            &root_bank,
        );

        let duplicate = handle_duplicate_shred(blockstore, shred, no_verify_chained_merkle_root)?;

        let should_mark_dead = migration_status
            .genesis_block()
            .is_some_and(|genesis| shred_slot > genesis.slot);
        if should_mark_dead {
            // Apart from Exists all existing cases mark dead inline in blockstore.
            // Once Alpenglow is active we can fully remove this thread and move Exists to inline as well.
            blockstore.set_dead_slot_if_duplicate_and_not_full(shred_slot)?;
        }

        let Some((shred1, shred2)) = duplicate else {
            return Ok(());
        };

        if migration_status.should_respond_to_ancestor_hashes_requests(shred_slot) {
            // In alpenglow we store the duplicate block proofs in blockstore for the purposes of slashing,
            // however we do not need to propagate the duplicate proof through gossip.
            // We still propagate during the mixed migration epoch, to account for other nodes that are stuck
            // and require a duplicate proof to proceed
            cluster_info.push_duplicate_shred(&shred1, &shred2)?;
        }

        if !migration_status.is_alpenglow_enabled() {
            // The state machine can be exited as soon as alpenglow is enabled.
            // Notify duplicate consensus state machine. If channel is full we wait.
            duplicate_slots_sender.send(shred_slot)?;
        }

        Ok(())
    };
    const RECV_TIMEOUT: Duration = Duration::from_millis(200);
    std::iter::once(shred_receiver.recv_timeout(RECV_TIMEOUT)?)
        .chain(shred_receiver.try_iter())
        .try_for_each(check_duplicate)
}

/// How an accepted data shred reached this node, for
/// `agave_data_set_completion_duration_us`/`agave_data_set_completions_total`.
/// Ordered so that a data set's label is the maximum over its data shreds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ShredOrigin {
    Turbine,
    Repair,
    /// Rebuilt by erasure recovery from this node's other shreds.
    Recovered,
}

impl ShredOrigin {
    fn as_label(self) -> &'static str {
        match self {
            Self::Turbine => "turbine",
            Self::Repair => "repair",
            Self::Recovered => "recovered",
        }
    }
}

/// A small, bounded, in-memory cache of recently-accepted data shred payloads,
/// keyed by `(slot, index)`. A data set's shreds usually arrive across several
/// `run_insert` calls, so this lets the call that completes a data set decode
/// it from memory -- it is the live replay path's only source of content (see
/// `ledger::live_replay_entries`); there is no RocksDB fallback. It is fed
/// exclusively with shreds `Blockstore` reports as *accepted* (never
/// duplicates or conflicts), so a cached payload is always the one stored.
///
/// Owned by the single `solWinInsert` thread, so no locking is needed.
struct ShredPayloadCache {
    /// Slots currently cached, oldest first; bounds memory regardless of
    /// per-slot shred counts.
    slots: VecDeque<Slot>,
    max_slots: usize,
    payloads: HashMap<Slot, HashMap<u32, (shred::Payload, ShredOrigin)>>,
    /// Partial data-set replay state per cached slot; `None` when
    /// `AGAVE_PARTIAL_DATA_SET_REPLAY` is off.
    partial: Option<HashMap<Slot, PartialSlotState>>,
    /// Every completed data set's classification, by (slot, start), in order.
    #[cfg(test)]
    classified_for_tests: Vec<(Slot, u32, DataSetClassification)>,
}

/// Partial data-set replay state of one slot.
#[derive(Default)]
struct PartialSlotState {
    /// Indices of cached data shreds that end a data set
    /// (`DATA_COMPLETE_SHRED`); the next data set starts right after each.
    data_set_ends: BTreeSet<u32>,
    /// Decoders of incomplete data sets, by data-set start index.
    decoders: HashMap<u32, PartialDecoder>,
    /// The slot's `SlotMeta::first_shred_timestamp` when first seen. It
    /// changes when the slot is cleared and received again.
    generation: Option<u64>,
    /// The slot was cleared and received again: no partial replay for it.
    disabled: bool,
}

struct PartialDecoder {
    decoder: PartialDataSetDecoder,
    /// When this data set's first leading entries were sent to replay.
    first_sent: Option<Instant>,
}

/// A data set an insert call completed, as partial replay's metrics need it.
struct CompletedDataSet {
    indices: Range<u32>,
    contents: CompletedContents,
}

enum CompletedContents {
    /// Transactions per entry.
    Entries(Vec<usize>),
    BlockMarker,
    /// Could not be decoded from memory (sent as `ReplayFromBlockstore`).
    NotDecoded,
}

/// A completed data set's decoder as it was before the completing call.
#[derive(Clone, Copy)]
struct DecoderSummary {
    next_index: u32,
    entries_returned: usize,
    stop_reason: Option<PartialDecoderStop>,
}

impl DecoderSummary {
    fn of(decoder: &PartialDataSetDecoder) -> Self {
        Self {
            next_index: decoder.next_index(),
            entries_returned: decoder.entries_returned(),
            stop_reason: decoder.stop_reason(),
        }
    }
}

/// Labels of `agave_partial_data_set_outcome_total` and
/// `agave_partial_txs_total` for one completed data set.
#[derive(Debug, PartialEq, Eq)]
struct DataSetClassification {
    outcome: &'static str,
    /// Transactions per class, in first-seen order; classes with none are
    /// left out.
    txs: Vec<(&'static str, u64)>,
}

impl DataSetClassification {
    fn add_txs(&mut self, class: &'static str, txs: usize) {
        if txs == 0 {
            return;
        }
        let txs = txs as u64;
        match self.txs.iter_mut().find(|(seen, _)| *seen == class) {
            Some((_, total)) => *total = total.saturating_add(txs),
            None => self.txs.push((class, txs)),
        }
    }

    fn record(&self) {
        solana_metrics::pipeline_metrics::PARTIAL_DATA_SET_OUTCOME_TOTAL
            .with_label_values(&[self.outcome])
            .inc();
        let txs_total = &solana_metrics::pipeline_metrics::PARTIAL_TXS_TOTAL;
        for (class, txs) in &self.txs {
            txs_total.with_label_values(&[class]).inc_by(*txs);
        }
    }
}

/// Why partial replay did or didn't send the completed data set `set`'s
/// transactions early (see `agave_partial_data_set_outcome_total` and
/// `agave_partial_txs_total` for the labels). `decoder` is its decoder before
/// the completing call, if it had one; `cached_before(index)` tells whether
/// the slot's shred `index` was cached before that call. Metrics only; the
/// counts come from leader data, so nothing here may index by them.
fn classify_completed_data_set(
    set: &CompletedDataSet,
    decoder: Option<DecoderSummary>,
    cached_before: impl Fn(u32) -> bool,
) -> DataSetClassification {
    let tx_counts = match &set.contents {
        CompletedContents::Entries(tx_counts) => tx_counts,
        CompletedContents::BlockMarker => {
            return DataSetClassification {
                outcome: "marker",
                txs: vec![],
            };
        }
        CompletedContents::NotDecoded => {
            return DataSetClassification {
                outcome: "not_decoded",
                txs: vec![],
            };
        }
    };
    let entries_returned = decoder
        .map_or(0, |decoder| decoder.entries_returned)
        .min(tx_counts.len());
    let outcome = if entries_returned > 0 {
        "early"
    } else if !set.indices.clone().any(&cached_before) {
        // Checked before the decoder: one is created for a data set as soon
        // as the previous one's last shred arrives, with nothing appended.
        "same_call"
    } else {
        match decoder {
            None => "no_decoder",
            Some(decoder) => match decoder.stop_reason {
                Some(PartialDecoderStop::Tick) => "first_entry_tick",
                Some(PartialDecoderStop::LastEntry) => "single_entry",
                Some(_) => "stopped",
                None if (decoder.next_index..set.indices.end).any(&cached_before) => "gap",
                None => "incomplete_entry",
            },
        }
    };
    let mut classification = DataSetClassification {
        outcome,
        txs: vec![],
    };
    if matches!(outcome, "same_call" | "no_decoder") {
        classification.add_txs(outcome, tx_counts.iter().sum());
        return classification;
    }
    let not_sent = match decoder.and_then(|decoder| decoder.stop_reason) {
        None | Some(PartialDecoderStop::Tick | PartialDecoderStop::LastEntry) => "not_reached",
        Some(_) => "stopped",
    };
    let first_tick = tx_counts.iter().position(|txs| *txs == 0);
    let last = tx_counts.len().saturating_sub(1);
    for (index, txs) in tx_counts.iter().enumerate() {
        let after_tick = first_tick.is_some_and(|tick| index > tick);
        let class = if index < entries_returned {
            "early"
        } else if index == last {
            if after_tick {
                "last_entry_after_tick"
            } else {
                "last_entry"
            }
        } else if after_tick {
            "after_tick"
        } else {
            not_sent
        };
        classification.add_txs(class, *txs);
    }
    classification
}

impl ShredPayloadCache {
    fn new(max_slots: usize, partial_data_set_replay: bool) -> Self {
        Self {
            slots: VecDeque::with_capacity(max_slots),
            max_slots,
            payloads: HashMap::new(),
            partial: partial_data_set_replay.then(HashMap::new),
            #[cfg(test)]
            classified_for_tests: Vec::new(),
        }
    }

    fn partial_data_set_replay(&self) -> bool {
        self.partial.is_some()
    }

    fn insert(&mut self, slot: Slot, index: u32, payload: shred::Payload, origin: ShredOrigin) {
        if !self.payloads.contains_key(&slot) {
            self.slots.push_back(slot);
            self.payloads.insert(slot, HashMap::new());
            while self.slots.len() > self.max_slots {
                if let Some(oldest) = self.slots.pop_front() {
                    self.payloads.remove(&oldest);
                    if let Some(partial) = self.partial.as_mut() {
                        partial.remove(&oldest);
                    }
                }
            }
        }
        if let Some(partial) = self.partial.as_mut()
            && shred::wire::get_flags(&payload)
                .is_ok_and(|flags| flags.contains(shred::ShredFlags::DATA_COMPLETE_SHRED))
        {
            partial.entry(slot).or_default().data_set_ends.insert(index);
        }
        self.payloads
            .get_mut(&slot)
            .unwrap()
            .insert(index, (payload, origin));
    }

    /// Partial data-set replay for `slot` after an insert call, outside the
    /// blockstore's insert lock: sends the newly decodable leading entries of
    /// the slot's incomplete data sets touched by this call's `accepted`
    /// indices to replay. `completed` are the data sets this call completed
    /// (and sent whole, or as `ReplayFromBlockstore`); each is classified for
    /// the partial replay metrics here. `generation` is the slot's current
    /// `SlotMeta::first_shred_timestamp`.
    fn send_partial_entries(
        &mut self,
        slot: Slot,
        generation: Option<u64>,
        accepted: &[u32],
        completed: &[CompletedDataSet],
        live_replay_sender: &LiveReplaySender,
    ) {
        let (Some(partial), Some(slot_payloads)) =
            (self.partial.as_mut(), self.payloads.get_mut(&slot))
        else {
            return;
        };
        let Some(generation) = generation else {
            return;
        };
        let state = partial.entry(slot).or_default();
        let partial_metrics = &solana_metrics::pipeline_metrics::PARTIAL_DATA_SET_TOTAL;
        match state.generation {
            None => state.generation = Some(generation),
            Some(seen) if seen != generation && !state.disabled => {
                // Cleared and received again: shreds cached before this call
                // belong to the discarded version. Drop them, and don't replay
                // this slot's data sets early at all.
                state.disabled = true;
                state.decoders.clear();
                state.data_set_ends.clear();
                slot_payloads.retain(|index, _| accepted.contains(index));
                partial_metrics
                    .with_label_values(&["slot_disabled_after_purge"])
                    .inc();
            }
            Some(_) => (),
        }
        if state.disabled {
            return;
        }

        // Shreds of this call, to tell what was cached before it.
        let accepted_in_call: HashSet<u32> = if completed.is_empty() {
            HashSet::new()
        } else {
            accepted.iter().copied().collect()
        };
        for set in completed {
            let removed = state.decoders.remove(&set.indices.start);
            if let Some(PartialDecoder {
                first_sent: Some(first_sent),
                ..
            }) = &removed
            {
                solana_metrics::pipeline_metrics::PARTIAL_DATA_SET_LEAD_US
                    .observe(first_sent.elapsed().as_micros() as f64);
            }
            let classification = classify_completed_data_set(
                set,
                removed
                    .as_ref()
                    .map(|removed| DecoderSummary::of(&removed.decoder)),
                |index| slot_payloads.contains_key(&index) && !accepted_in_call.contains(&index),
            );
            classification.record();
            #[cfg(test)]
            self.classified_for_tests
                .push((slot, set.indices.start, classification));
        }

        // Start of the data set each accepted shred belongs to (as far as the
        // cached data-set ends tell; a walk never crosses a missing end), and
        // of the data set after each accepted data-set end.
        let mut starts = BTreeSet::new();
        for index in accepted {
            let start = state
                .data_set_ends
                .range(..*index)
                .next_back()
                .map_or(0, |end| end.saturating_add(1));
            starts.insert(start);
            if state.data_set_ends.contains(index) {
                starts.insert(index.saturating_add(1));
            }
        }
        for start in starts {
            if completed.iter().any(|set| set.indices.start == start) {
                continue;
            }
            let entry = state
                .decoders
                .entry(start)
                .or_insert_with(|| PartialDecoder {
                    decoder: PartialDataSetDecoder::new(start),
                    first_sent: None,
                });
            if entry.decoder.is_stopped() {
                continue;
            }
            loop {
                let index = entry.decoder.next_index();
                // The data set's last shred completes it; it is replayed whole.
                if state.data_set_ends.contains(&index) {
                    break;
                }
                let Some((payload, _)) = slot_payloads.get(&index) else {
                    break;
                };
                if !entry.decoder.append(payload) {
                    break;
                }
            }
            if let Some((first_entry, entries)) = entry.decoder.take_new_entries() {
                partial_metrics.with_label_values(&["chunk_sent"]).inc();
                partial_metrics
                    .with_label_values(&["entries_sent"])
                    .inc_by(entries.len() as u64);
                entry.first_sent.get_or_insert_with(Instant::now);
                // `ready` point of the early-replayed txs in this chunk (taken
                // before the send, so it never follows replay's pickup).
                solana_metrics::pipeline_latency::TX_PIPELINE_LATENCY.mark_data_set_chunk_ready(
                    slot,
                    start,
                    first_entry,
                    Instant::now(),
                );
                let _ = live_replay_sender.send(LiveReplayMessage::PartialEntries {
                    slot,
                    start,
                    generation,
                    first_entry,
                    entries,
                });
            }
            if entry.decoder.is_stopped() {
                partial_metrics
                    .with_label_values(&["decoder_stopped"])
                    .inc();
                if let Some(reason) = entry.decoder.stop_reason() {
                    solana_metrics::pipeline_metrics::PARTIAL_DECODER_STOP_TOTAL
                        .with_label_values(&[reason.as_label()])
                        .inc();
                }
            }
        }
    }

    /// Returns the raw wire bytes for `indices.start..indices.end`, in index
    /// order, only if every one of them is present in the cache for `slot`,
    /// together with the data set's origin (the maximum over its shreds).
    /// `Payload`'s inner `Bytes` makes this a refcount bump per shred.
    fn get_contiguous(
        &self,
        slot: Slot,
        indices: &Range<u32>,
    ) -> Option<(Vec<shred::Payload>, ShredOrigin)> {
        let slot_payloads = self.payloads.get(&slot)?;
        let mut origin = ShredOrigin::Turbine;
        let payloads = indices
            .clone()
            .map(|index| {
                let (payload, shred_origin) = slot_payloads.get(&index)?;
                origin = origin.max(*shred_origin);
                Some(payload.clone())
            })
            .collect::<Option<Vec<_>>>()?;
        Some((payloads, origin))
    }
}

#[allow(clippy::too_many_arguments)]
fn run_insert<'db, F>(
    thread_pool: &ThreadPool,
    verified_receiver: &Receiver<Vec<(shred::Payload, /*is_repaired:*/ bool, BlockLocation)>>,
    blockstore: &'db Blockstore,
    shred_recovery_context: &mut ShredRecoveryContext,
    pinnable_slice: &mut DBPinnableSlice<'db>,
    write_batch: &mut WriteBatch,
    handle_duplicate: F,
    metrics: &mut BlockstoreInsertionMetrics,
    ws_metrics: &mut WindowServiceMetrics,
    completed_data_sets_sender: Option<&CompletedDataSetsSender>,
    live_replay_sender: &LiveReplaySender,
    shred_payload_cache: &mut ShredPayloadCache,
) -> Result<()>
where
    F: Fn(PossibleDuplicateShred),
{
    const RECV_TIMEOUT: Duration = Duration::from_millis(200);
    let mut shred_receiver_elapsed = Measure::start("shred_receiver_elapsed");
    let mut shreds = verified_receiver.recv_timeout(RECV_TIMEOUT)?;
    // When this batch reached window_service; a data set this call completes
    // has its `fetch_to_data_set_ready` span split here.
    let batch_received = Instant::now();
    solana_metrics::pipeline_metrics::VERIFIED_SHREDS_QUEUE_LENGTH
        .set(verified_receiver.len() as f64);
    shreds.extend(verified_receiver.try_iter().flatten());
    shred_receiver_elapsed.stop();
    ws_metrics.shred_receiver_elapsed_us += shred_receiver_elapsed.as_us();
    solana_metrics::pipeline_metrics::SHRED_STAGE_DURATION_US
        .with_label_values(&["verified_recv_wait"])
        .observe(shred_receiver_elapsed.as_us() as f64);
    ws_metrics.run_insert_count += 1;
    let handle_shred = |(shred, repair, block_location): (shred::Payload, bool, BlockLocation)| {
        if repair {
            ws_metrics.num_repairs.fetch_add(1, Ordering::Relaxed);
        }
        let shred = Shred::new_from_serialized_shred(shred).ok()?;
        Some((Cow::Owned(shred), repair, block_location))
    };
    let now = Instant::now();
    let shreds: Vec<(Cow<'_, Shred>, bool, BlockLocation)> = thread_pool.install(|| {
        shreds
            .into_par_iter()
            .with_min_len(32)
            .filter_map(handle_shred)
            .collect()
    });
    ws_metrics.handle_packets_elapsed_us += now.elapsed().as_micros() as u64;
    ws_metrics.num_shreds_received += shreds.len();
    solana_metrics::pipeline_metrics::SHRED_STAGE_DURATION_US
        .with_label_values(&["deserialize"])
        .observe(now.elapsed().as_micros() as f64);

    // This call's received Original data shreds, and whether each came
    // through repair. A data shred the blockstore accepts below that is not in
    // here was rebuilt by erasure recovery. If a batch holds the same shred
    // twice, the first copy wins, as in the blockstore, which inserts the
    // first copy it processes and rejects the second as already present.
    let mut received_data_shreds: HashMap<(Slot, u32), /*is_repaired:*/ bool> =
        HashMap::with_capacity(shreds.len());
    for (shred, repair, block_location) in &shreds {
        if *block_location == BlockLocation::Original && shred.is_data() {
            received_data_shreds
                .entry((shred.slot(), shred.index()))
                .or_insert(*repair);
        }
    }

    let store_start = Instant::now();
    let before_insert_shreds_us = metrics.insert_shreds_elapsed_us;
    let before_write_batch_us = metrics.write_batch_elapsed_us;
    let before_recovery_us = metrics.shred_recovery_elapsed_us;
    let before_insert_lock_us = metrics.insert_lock_elapsed_us;
    let before_commit_working_sets_us = metrics.commit_working_sets_elapsed_us;
    let before_index_meta_us = metrics.index_meta_time_us;
    let before_num_shreds = metrics.num_shreds;
    let before_num_inserted = metrics.num_inserted;

    // Runs inside `Blockstore`'s insertion call after the in-memory work and
    // *before* its RocksDB write: decode every data set this call completed
    // and send it to ReplayStage. That send is also replay's wake-up, so
    // replay proceeds without waiting for the write. Everything it needs is
    // either in `accepted` (this call's accepted data shreds, including
    // erasure-recovered ones) or in `shred_payload_cache` (accepted by an
    // earlier call).
    //
    // If a data set can't be assembled from memory (a sibling shred was
    // persisted before a restart, or aged out of `shred_payload_cache`),
    // tell replay to read the rest of that slot from the persisted
    // blockstore. This is the rare recovery path; the steady state is the
    // channel. It never marks a slot dead -- a genuinely corrupt data set
    // surfaces through the normal blockstore read path.
    //
    // With partial data-set replay on, this hook only records what it saw;
    // the partial decoding runs after the insert call returns, outside the
    // blockstore's insert lock.
    let partial_data_set_replay = shred_payload_cache.partial_data_set_replay();
    let mut partial_accepted: BTreeMap<Slot, Vec<u32>> = BTreeMap::new();
    let mut partial_completed: HashMap<Slot, Vec<CompletedDataSet>> = HashMap::new();
    let mut before_write = |completed: &[CompletedDataSetInfo],
                            accepted: &[(Slot, u32, shred::Payload)]| {
        for (slot, index, payload) in accepted {
            let origin = match received_data_shreds.get(&(*slot, *index)) {
                Some(false) => ShredOrigin::Turbine,
                Some(true) => ShredOrigin::Repair,
                None => ShredOrigin::Recovered,
            };
            shred_payload_cache.insert(*slot, *index, payload.clone(), origin);
            if partial_data_set_replay {
                partial_accepted.entry(*slot).or_default().push(*index);
            }
        }
        for info in completed {
            if info.location != BlockLocation::Original {
                continue;
            }
            let decoded = shred_payload_cache
                .get_contiguous(info.slot, &info.indices)
                .and_then(|(payloads, origin)| {
                    Blockstore::decode_completed_data_set_from_payloads(payloads)
                        .map(|component| (component, origin))
                });
            if partial_data_set_replay {
                let contents = match &decoded {
                    Some((ParsedBlockComponent::EntryBatch(entries), _)) => {
                        CompletedContents::Entries(
                            entries
                                .iter()
                                .map(|entry| entry.transactions.len())
                                .collect(),
                        )
                    }
                    Some((ParsedBlockComponent::BlockMarker(_), _)) => {
                        CompletedContents::BlockMarker
                    }
                    None => CompletedContents::NotDecoded,
                };
                partial_completed
                    .entry(info.slot)
                    .or_default()
                    .push(CompletedDataSet {
                        indices: info.indices.clone(),
                        contents,
                    });
            }
            match decoded {
                Some((component, origin)) => {
                    let _ = live_replay_sender.send(LiveReplayMessage::Entries {
                        slot: info.slot,
                        indices: info.indices.clone(),
                        component,
                        slot_full: info.slot_full,
                    });
                    let tracker = &solana_metrics::pipeline_latency::TX_PIPELINE_LATENCY;
                    tracker.mark_data_set_ready(info.slot, info.indices.start);
                    tracker.mark_data_set_completed(
                        info.slot,
                        info.indices.start,
                        batch_received,
                        origin.as_label(),
                    );
                }
                None => {
                    warn!(
                        "completed data set {:?} of slot {} not assembled from memory; replay \
                         will read this slot from the blockstore",
                        info.indices, info.slot
                    );
                    let _ = live_replay_sender
                        .send(LiveReplayMessage::ReplayFromBlockstore { slot: info.slot });
                }
            }
        }
    };

    let completed_data_sets = blockstore.insert_shreds_at_location_handle_duplicate(
        shreds,
        false, // is_trusted
        shred_recovery_context,
        pinnable_slice,
        write_batch,
        &handle_duplicate,
        metrics,
        Some(&mut before_write),
    )?;

    for (slot, accepted) in &partial_accepted {
        // Tags the chunks with the slot's current version; replay only takes
        // chunks whose tag matches the blockstore's at that time.
        // 0: the slot holds no shreds (e.g. just cleared), so no version.
        let generation = blockstore
            .meta(*slot)
            .ok()
            .flatten()
            .map(|meta| meta.first_shred_timestamp)
            .filter(|timestamp| *timestamp != 0);
        let completed = partial_completed
            .get(slot)
            .map(Vec::as_slice)
            .unwrap_or_default();
        shred_payload_cache.send_partial_entries(
            *slot,
            generation,
            accepted,
            completed,
            live_replay_sender,
        );
    }

    let blockstore_store_us = &solana_metrics::pipeline_metrics::BLOCKSTORE_STORE_DURATION_US;
    blockstore_store_us
        .with_label_values(&["total"])
        .observe(store_start.elapsed().as_micros() as f64);
    blockstore_store_us
        .with_label_values(&["insert_shreds"])
        .observe((metrics.insert_shreds_elapsed_us - before_insert_shreds_us) as f64);
    blockstore_store_us
        .with_label_values(&["write_batch"])
        .observe((metrics.write_batch_elapsed_us - before_write_batch_us) as f64);
    blockstore_store_us
        .with_label_values(&["recovery"])
        .observe((metrics.shred_recovery_elapsed_us - before_recovery_us) as f64);
    blockstore_store_us
        .with_label_values(&["insert_lock"])
        .observe((metrics.insert_lock_elapsed_us - before_insert_lock_us) as f64);
    blockstore_store_us
        .with_label_values(&["commit_working_sets"])
        .observe((metrics.commit_working_sets_elapsed_us - before_commit_working_sets_us) as f64);
    blockstore_store_us
        .with_label_values(&["index_meta"])
        .observe((metrics.index_meta_time_us - before_index_meta_us) as f64);
    let insert_shreds_total = &solana_metrics::pipeline_metrics::BLOCKSTORE_INSERT_SHREDS_TOTAL;
    insert_shreds_total
        .with_label_values(&["received"])
        .inc_by((metrics.num_shreds - before_num_shreds) as u64);
    insert_shreds_total
        .with_label_values(&["inserted"])
        .inc_by(metrics.num_inserted - before_num_inserted);

    if let Some(sender) = completed_data_sets_sender {
        sender.try_send(completed_data_sets)?;
    }

    Ok(())
}

pub struct WindowServiceChannels {
    pub verified_receiver: Receiver<Vec<(shred::Payload, /*is_repaired:*/ bool, BlockLocation)>>,
    pub retransmit_sender: EvictingSender<Vec<shred::Payload>>,
    pub completed_data_sets_sender: Option<CompletedDataSetsSender>,
    pub live_replay_sender: LiveReplaySender,
    pub duplicate_slots_sender: DuplicateSlotSender,
    pub repair_service_channels: RepairServiceChannels,
    pub block_id_repair_channels: BlockIdRepairChannels,
}

impl WindowServiceChannels {
    pub fn new(
        verified_receiver: Receiver<Vec<(shred::Payload, /*is_repaired:*/ bool, BlockLocation)>>,
        retransmit_sender: EvictingSender<Vec<shred::Payload>>,
        completed_data_sets_sender: Option<CompletedDataSetsSender>,
        live_replay_sender: LiveReplaySender,
        duplicate_slots_sender: DuplicateSlotSender,
        repair_service_channels: RepairServiceChannels,
        block_id_repair_channels: BlockIdRepairChannels,
    ) -> Self {
        Self {
            verified_receiver,
            retransmit_sender,
            completed_data_sets_sender,
            live_replay_sender,
            duplicate_slots_sender,
            repair_service_channels,
            block_id_repair_channels,
        }
    }
}

pub(crate) struct WindowService {
    t_insert: JoinHandle<()>,
    t_check_duplicate: JoinHandle<()>,
    repair_service: RepairService,
    block_id_repair_service: BlockIdRepairService,
}

impl WindowService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        blockstore: Arc<Blockstore>,
        repair_socket: Arc<UdpSocket>,
        ancestor_hashes_socket: Arc<UdpSocket>,
        block_id_repair_socket: Arc<UdpSocket>,
        exit: Arc<AtomicBool>,
        repair_info: RepairInfo,
        window_service_channels: WindowServiceChannels,
        shred_version: u16,
        outstanding_repair_requests: Arc<RwLock<OutstandingShredRepairs>>,
        repair_xdp_sender: Option<PinnedXdpSender>,
    ) -> WindowService {
        let cluster_info = repair_info.cluster_info.clone();
        let bank_forks = repair_info.bank_forks.clone();

        let WindowServiceChannels {
            verified_receiver,
            retransmit_sender,
            completed_data_sets_sender,
            live_replay_sender,
            duplicate_slots_sender,
            repair_service_channels,
            block_id_repair_channels,
        } = window_service_channels;

        let repair_service = RepairService::new(
            blockstore.clone(),
            exit.clone(),
            repair_socket.clone(),
            ancestor_hashes_socket,
            repair_info.clone(),
            outstanding_repair_requests.clone(),
            repair_service_channels,
            repair_xdp_sender,
        );

        let block_id_repair_service = BlockIdRepairService::new(
            exit.clone(),
            blockstore.clone(),
            block_id_repair_socket,
            repair_socket,
            block_id_repair_channels,
            repair_info,
            outstanding_repair_requests,
        );

        let (duplicate_sender, duplicate_receiver) = unbounded();

        let t_check_duplicate = Self::start_check_duplicate_thread(
            cluster_info,
            exit.clone(),
            blockstore.clone(),
            duplicate_receiver,
            duplicate_slots_sender,
            bank_forks.clone(),
        );

        let sharable_banks = bank_forks.read().unwrap().sharable_banks();
        let t_insert = Self::start_window_insert_thread(
            exit,
            blockstore,
            sharable_banks,
            shred_version,
            verified_receiver,
            duplicate_sender,
            completed_data_sets_sender,
            live_replay_sender,
            retransmit_sender,
        );

        WindowService {
            t_insert,
            t_check_duplicate,
            repair_service,
            block_id_repair_service,
        }
    }

    fn start_check_duplicate_thread(
        cluster_info: Arc<ClusterInfo>,
        exit: Arc<AtomicBool>,
        blockstore: Arc<Blockstore>,
        duplicate_receiver: Receiver<PossibleDuplicateShred>,
        duplicate_slots_sender: DuplicateSlotSender,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("solWinCheckDup".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    if let Err(e) = run_check_duplicate(
                        &cluster_info,
                        &blockstore,
                        &duplicate_receiver,
                        &duplicate_slots_sender,
                        &bank_forks,
                    ) && Self::should_exit_on_error(e)
                    {
                        break;
                    }
                }
            })
            .unwrap()
    }

    fn start_window_insert_thread(
        exit: Arc<AtomicBool>,
        blockstore: Arc<Blockstore>,
        sharable_banks: SharableBanks,
        shred_version: u16,
        verified_receiver: Receiver<Vec<(shred::Payload, /*is_repaired:*/ bool, BlockLocation)>>,
        check_duplicate_sender: Sender<PossibleDuplicateShred>,
        completed_data_sets_sender: Option<CompletedDataSetsSender>,
        live_replay_sender: LiveReplaySender,
        retransmit_sender: EvictingSender<Vec<shred::Payload>>,
    ) -> JoinHandle<()> {
        let reed_solomon_cache = ReedSolomonCache::default();
        Builder::new()
            .name("solWinInsert".to_string())
            .spawn(move || {
                let thread_pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(get_thread_count().min(8))
                    // Use the current thread as one of the workers. This reduces overhead when the
                    // pool is used to process a small number of shreds, since they'll be processed
                    // directly on the current thread.
                    .use_current_thread()
                    .thread_name(|i| format!("solWinInsert{i:02}"))
                    .build()
                    .unwrap();
                let handle_duplicate = |possible_duplicate_shred| {
                    let _ = check_duplicate_sender.send(possible_duplicate_shred);
                };

                const METRICS_REPORTING_INTERVAL: Duration = Duration::from_secs(2);
                let mut metrics = BlockstoreInsertionMetrics::default();
                let mut ws_metrics = WindowServiceMetrics::default();
                let mut last_print = Instant::now();
                let mut shred_recovery_context = ShredRecoveryContext::new(
                    reed_solomon_cache,
                    retransmit_sender,
                    sharable_banks.root(),
                    shred_version,
                );
                let mut pinnable_slice = blockstore.new_pinnable_slice();
                let mut write_batch = blockstore.get_write_batch();
                // Partial data-set replay: replay a data set's leading entries
                // before all its shreds arrive. Off unless explicitly enabled.
                let partial_data_set_replay =
                    std::env::var("AGAVE_PARTIAL_DATA_SET_REPLAY").is_ok_and(|value| value == "1");
                solana_metrics::pipeline_metrics::PARTIAL_DATA_SET_REPLAY_ENABLED
                    .set(if partial_data_set_replay { 1.0 } else { 0.0 });
                if partial_data_set_replay {
                    warn!(
                        "partial data-set replay enabled (AGAVE_PARTIAL_DATA_SET_REPLAY=1): \
                         replay may start on a data set before all its shreds arrive. Keep \
                         this off on voting nodes."
                    );
                }
                // 64 slots is deliberately generous: this is the ONLY source live
                // replay has for a completed data set's content, so a data set whose
                // payloads are evicted before it completes gets its slot marked dead.
                let mut shred_payload_cache = ShredPayloadCache::new(64, partial_data_set_replay);

                while !exit.load(Ordering::Relaxed) {
                    shred_recovery_context.maybe_update(sharable_banks.root());

                    if let Err(e) = run_insert(
                        &thread_pool,
                        &verified_receiver,
                        &blockstore,
                        &mut shred_recovery_context,
                        &mut pinnable_slice,
                        &mut write_batch,
                        handle_duplicate,
                        &mut metrics,
                        &mut ws_metrics,
                        completed_data_sets_sender.as_ref(),
                        &live_replay_sender,
                        &mut shred_payload_cache,
                    ) {
                        ws_metrics.record_error(&e);
                        if Self::should_exit_on_error(e) {
                            break;
                        }
                    }

                    if last_print.elapsed() > METRICS_REPORTING_INTERVAL {
                        metrics.report_metrics();
                        metrics = BlockstoreInsertionMetrics::default();
                        ws_metrics.report_metrics();
                        ws_metrics = WindowServiceMetrics::default();
                        last_print = Instant::now();
                    }
                    shred_recovery_context.maybe_submit_stats();
                }
            })
            .unwrap()
    }

    fn should_exit_on_error(e: Error) -> bool {
        match e {
            Error::RecvTimeout(RecvTimeoutError::Disconnected) => true,
            Error::RecvTimeout(RecvTimeoutError::Timeout) => false,
            Error::Send => true,
            _ => {
                let version = solana_version::version!();
                datapoint_error!(
                    "error",
                    ("thread", thread::current().name().unwrap_or("?"), String),
                    ("message", format!("{e}"), String),
                    ("version", version, String)
                );
                error!("thread {:?} error {:?}", thread::current().name(), e);
                false
            }
        }
    }

    pub(crate) fn join(self) -> thread::Result<()> {
        self.t_insert.join()?;
        self.t_check_duplicate.join()?;
        self.repair_service.join()?;
        self.block_id_repair_service.join()
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        crossbeam_channel::bounded,
        rand::Rng,
        solana_entry::{
            block_component::ParsedBlockComponent,
            entry::{Entry, EntryView, create_ticks},
        },
        solana_gossip::contact_info::ContactInfo,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_ledger::{
            blockstore::{Blockstore, make_many_slot_entries},
            genesis_utils::create_genesis_config,
            get_tmp_ledger_path_auto_delete,
            shred::{ProcessShredsStats, Shredder},
        },
        solana_net_utils::SocketAddrSpace,
        solana_runtime::bank::Bank,
        solana_signer::Signer,
        solana_time_utils::timestamp,
    };

    fn local_entries_to_shred(
        entries: &[Entry],
        slot: Slot,
        parent: Slot,
        keypair: &Keypair,
    ) -> Vec<Shred> {
        let shredder = Shredder::new(slot, parent, 0, 0).unwrap();
        let (data_shreds, _) = shredder.entries_to_merkle_shreds_for_tests(
            keypair,
            entries,
            true, // is_last_in_slot
            // chained_merkle_root
            Hash::new_from_array(rand::rng().random()),
            0, // next_shred_index
            0, // next_code_index
            &ReedSolomonCache::default(),
            &mut ProcessShredsStats::default(),
        );
        data_shreds
    }

    #[test]
    fn test_process_shred() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());
        let num_entries = 10;
        let original_entries = create_ticks(num_entries, 0, Hash::default());
        let mut shreds = local_entries_to_shred(&original_entries, 0, 0, &Keypair::new());
        shreds.reverse();
        blockstore
            .insert_shreds(shreds, false)
            .expect("Expect successful processing of shred");

        assert_eq!(blockstore.get_slot_entries(0, 0).unwrap(), original_entries);
    }

    #[test]
    fn test_shred_payload_cache_data_set_origin() {
        let mut cache = ShredPayloadCache::new(2, false);
        let payload = |byte: u8| shred::Payload::from(vec![byte]);
        cache.insert(5, 0, payload(0), ShredOrigin::Turbine);
        cache.insert(5, 1, payload(1), ShredOrigin::Repair);
        cache.insert(5, 2, payload(2), ShredOrigin::Turbine);

        let (payloads, origin) = cache.get_contiguous(5, &(0..3)).unwrap();
        assert_eq!(payloads.len(), 3);
        assert_eq!(origin, ShredOrigin::Repair);
        assert_eq!(
            cache.get_contiguous(5, &(0..1)).unwrap().1,
            ShredOrigin::Turbine
        );

        cache.insert(5, 3, payload(3), ShredOrigin::Recovered);
        assert_eq!(
            cache.get_contiguous(5, &(1..4)).unwrap().1,
            ShredOrigin::Recovered
        );
        // A missing index means the data set can't be assembled.
        assert!(cache.get_contiguous(5, &(3..5)).is_none());
    }

    #[test]
    fn test_run_insert_tags_shred_origins() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path()).unwrap();
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank_forks = BankForks::new_rw_arc(Bank::new_for_tests(&genesis_config));
        let keypair = Keypair::new();
        let (slot, parent) = (1, 0);
        let shredder = Shredder::new(slot, parent, 0, 0).unwrap();
        let entries = create_ticks(10, 0, Hash::default());
        let (data_shreds, coding_shreds) = shredder.entries_to_merkle_shreds_for_tests(
            &keypair,
            &entries,
            true, // is_last_in_slot
            Hash::new_from_array(rand::rng().random()),
            0, // next_shred_index
            0, // next_code_index
            &ReedSolomonCache::default(),
            &mut ProcessShredsStats::default(),
        );
        assert!(data_shreds.len() >= 3);
        assert!(coding_shreds.len() >= data_shreds.len());

        // Data shred 0 never arrives, so erasure recovery rebuilds it; data
        // shred 1 arrives through repair; everything else through turbine.
        let missing = data_shreds[0].index();
        let repaired = data_shreds[1].index();
        let batch: Vec<_> = data_shreds[1..]
            .iter()
            .map(|shred| (shred.payload().clone(), shred.index() == repaired))
            .chain(
                coding_shreds
                    .iter()
                    .map(|shred| (shred.payload().clone(), false)),
            )
            .map(|(payload, repair)| (payload, repair, BlockLocation::Original))
            .collect();
        let (verified_sender, verified_receiver) = unbounded();
        verified_sender.send(batch).unwrap();

        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let (retransmit_sender, _retransmit_receiver) = EvictingSender::new_bounded(1024);
        let mut shred_recovery_context = ShredRecoveryContext::new(
            ReedSolomonCache::default(),
            retransmit_sender,
            bank_forks.read().unwrap().root_bank(),
            0, // shred_version
        );
        let mut pinnable_slice = blockstore.new_pinnable_slice();
        let mut write_batch = blockstore.get_write_batch();
        let (live_replay_sender, live_replay_receiver) = unbounded();
        let mut shred_payload_cache = ShredPayloadCache::new(4, false);
        run_insert(
            &thread_pool,
            &verified_receiver,
            &blockstore,
            &mut shred_recovery_context,
            &mut pinnable_slice,
            &mut write_batch,
            |_| {},
            &mut BlockstoreInsertionMetrics::default(),
            &mut WindowServiceMetrics::default(),
            None,
            &live_replay_sender,
            &mut shred_payload_cache,
        )
        .unwrap();

        let origins = &shred_payload_cache.payloads[&slot];
        assert_eq!(origins[&missing].1, ShredOrigin::Recovered);
        assert_eq!(origins[&repaired].1, ShredOrigin::Repair);
        for shred in &data_shreds[2..] {
            assert_eq!(origins[&shred.index()].1, ShredOrigin::Turbine);
        }
        // The slot's data set completed in this call and went to replay.
        assert!(live_replay_receiver.try_iter().count() > 0);
    }

    /// Data shreds of one data set for slot 1: six entries of six transfers
    /// each (so transactions span shred boundaries), then a tick.
    fn transfer_data_set_shreds(keypair: &Keypair) -> Vec<Shred> {
        let mut hash = Hash::new_unique();
        let mut entries: Vec<Entry> = (0..6u64)
            .map(|i| {
                let transactions = (0..6u64)
                    .map(|j| {
                        solana_system_transaction::transfer(
                            &Keypair::new(),
                            &solana_pubkey::Pubkey::new_unique(),
                            i * 10 + j + 1,
                            Hash::default(),
                        )
                    })
                    .collect();
                solana_entry::entry::next_entry_mut(&mut hash, 1, transactions)
            })
            .collect();
        entries.push(solana_entry::entry::next_entry_mut(&mut hash, 1, vec![]));
        Shredder::new(1, 0, 0, 0)
            .unwrap()
            .entries_to_merkle_shreds_for_tests(
                keypair,
                &entries,
                false, // is_last_in_slot
                Hash::new_from_array(rand::rng().random()),
                0, // next_shred_index
                0, // next_code_index
                &ReedSolomonCache::default(),
                &mut ProcessShredsStats::default(),
            )
            .0
    }

    /// One `run_insert` call on `batch`, received through turbine.
    fn run_insert_for_tests(
        blockstore: &Blockstore,
        bank_forks: &RwLock<BankForks>,
        batch: &[Shred],
        shred_payload_cache: &mut ShredPayloadCache,
        live_replay_sender: &LiveReplaySender,
    ) {
        let (verified_sender, verified_receiver) = unbounded();
        verified_sender
            .send(
                batch
                    .iter()
                    .map(|shred| (shred.payload().clone(), false, BlockLocation::Original))
                    .collect(),
            )
            .unwrap();
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let (retransmit_sender, _retransmit_receiver) = EvictingSender::new_bounded(1024);
        let mut shred_recovery_context = ShredRecoveryContext::new(
            ReedSolomonCache::default(),
            retransmit_sender,
            bank_forks.read().unwrap().root_bank(),
            0, // shred_version
        );
        let mut pinnable_slice = blockstore.new_pinnable_slice();
        let mut write_batch = blockstore.get_write_batch();
        run_insert(
            &thread_pool,
            &verified_receiver,
            blockstore,
            &mut shred_recovery_context,
            &mut pinnable_slice,
            &mut write_batch,
            |_| {},
            &mut BlockstoreInsertionMetrics::default(),
            &mut WindowServiceMetrics::default(),
            None,
            live_replay_sender,
            shred_payload_cache,
        )
        .unwrap();
    }

    fn entry_batch(component: ParsedBlockComponent) -> Vec<EntryView<bytes::Bytes>> {
        match component {
            ParsedBlockComponent::EntryBatch(entries) => entries,
            ParsedBlockComponent::BlockMarker(_) => panic!("expected EntryBatch"),
        }
    }

    #[test]
    fn test_run_insert_sends_partial_entries_then_the_completed_data_set() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path()).unwrap();
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank_forks = BankForks::new_rw_arc(Bank::new_for_tests(&genesis_config));
        let shreds = transfer_data_set_shreds(&Keypair::new());
        assert!(shreds.len() > 3);
        let full = entry_batch(
            Blockstore::decode_completed_data_set_from_payloads(
                shreds.iter().map(|shred| shred.payload().clone()).collect(),
            )
            .unwrap(),
        );

        let (live_replay_sender, live_replay_receiver) = unbounded();
        let mut cache = ShredPayloadCache::new(4, true);
        // Every shred but the data set's last, one insert call each.
        for shred in &shreds[..shreds.len() - 1] {
            run_insert_for_tests(
                &blockstore,
                &bank_forks,
                std::slice::from_ref(shred),
                &mut cache,
                &live_replay_sender,
            );
        }
        let generation = blockstore.meta(1).unwrap().unwrap().first_shred_timestamp;
        let mut partial = vec![];
        for message in live_replay_receiver.try_iter() {
            let LiveReplayMessage::PartialEntries {
                slot,
                start,
                generation: message_generation,
                first_entry,
                entries,
            } = message
            else {
                panic!("expected only partial entries before the data set completes");
            };
            assert_eq!(
                (slot, start, message_generation, first_entry),
                (1, 0, generation, partial.len())
            );
            partial.extend(entries);
        }
        assert!(!partial.is_empty());
        assert!(partial.len() < full.len());
        for (early, completed) in partial.iter().zip(&full) {
            assert_eq!(early.num_hashes, completed.num_hashes);
            assert_eq!(early.hash, completed.hash);
            assert_eq!(early.transactions.len(), completed.transactions.len());
            for (early_tx, completed_tx) in early.transactions.iter().zip(&completed.transactions) {
                assert_eq!(early_tx.data(), completed_tx.data());
            }
        }

        // The last shred completes the data set, which is sent whole.
        run_insert_for_tests(
            &blockstore,
            &bank_forks,
            std::slice::from_ref(shreds.last().unwrap()),
            &mut cache,
            &live_replay_sender,
        );
        let messages: Vec<_> = live_replay_receiver.try_iter().collect();
        assert_eq!(messages.len(), 1);
        let LiveReplayMessage::Entries {
            slot: 1,
            ref indices,
            ..
        } = messages[0]
        else {
            panic!("expected the completed data set");
        };
        assert_eq!(*indices, 0..shreds.len() as u32);
        // The completed data set's decoder is gone (the next data set, which
        // starts right after it, may already have one).
        assert!(
            !cache.partial.as_ref().unwrap()[&1]
                .decoders
                .contains_key(&0)
        );

        // Its classification: the early txs are exactly those sent, and the
        // classes sum to all its txs.
        let early_txs: usize = partial.iter().map(|entry| entry.transactions.len()).sum();
        let total_txs: usize = full.iter().map(|entry| entry.transactions.len()).sum();
        let [(1, 0, classification)] = &cache.classified_for_tests[..] else {
            panic!("expected one classified data set");
        };
        assert_eq!(classification.outcome, "early");
        assert_eq!(classified_txs(classification, "early"), early_txs as u64);
        assert_eq!(classified_total(classification), total_txs as u64);
        // The last entry is the data set's only tick, so the rest wasn't
        // reached yet.
        assert_eq!(
            classified_txs(classification, "not_reached"),
            (total_txs - early_txs) as u64
        );
    }

    fn classified_txs(classification: &DataSetClassification, class: &str) -> u64 {
        classification
            .txs
            .iter()
            .find(|(seen, _)| *seen == class)
            .map_or(0, |(_, txs)| *txs)
    }

    fn classified_total(classification: &DataSetClassification) -> u64 {
        classification.txs.iter().map(|(_, txs)| txs).sum()
    }

    #[test]
    fn test_run_insert_classifies_a_data_set_completed_in_one_call() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path()).unwrap();
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank_forks = BankForks::new_rw_arc(Bank::new_for_tests(&genesis_config));
        let shreds = transfer_data_set_shreds(&Keypair::new());
        let (live_replay_sender, _live_replay_receiver) = unbounded();
        let mut cache = ShredPayloadCache::new(4, true);
        run_insert_for_tests(
            &blockstore,
            &bank_forks,
            &shreds,
            &mut cache,
            &live_replay_sender,
        );
        assert_eq!(
            cache.classified_for_tests,
            vec![(
                1,
                0,
                DataSetClassification {
                    outcome: "same_call",
                    txs: vec![("same_call", 36)],
                }
            )]
        );
    }

    #[test]
    fn test_run_insert_classifies_a_data_set_whose_first_shred_came_last() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path()).unwrap();
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank_forks = BankForks::new_rw_arc(Bank::new_for_tests(&genesis_config));
        let shreds = transfer_data_set_shreds(&Keypair::new());
        assert!(shreds.len() > 3);
        let (live_replay_sender, _live_replay_receiver) = unbounded();
        let mut cache = ShredPayloadCache::new(4, true);
        // Everything but the first and last shreds, then those two.
        run_insert_for_tests(
            &blockstore,
            &bank_forks,
            &shreds[1..shreds.len() - 1],
            &mut cache,
            &live_replay_sender,
        );
        assert!(cache.classified_for_tests.is_empty());
        run_insert_for_tests(
            &blockstore,
            &bank_forks,
            &[shreds[0].clone(), shreds.last().unwrap().clone()],
            &mut cache,
            &live_replay_sender,
        );
        assert_eq!(
            cache.classified_for_tests,
            vec![(
                1,
                0,
                DataSetClassification {
                    outcome: "gap",
                    txs: vec![("not_reached", 36)],
                }
            )]
        );
    }

    fn entries_set(tx_counts: &[usize]) -> CompletedDataSet {
        CompletedDataSet {
            indices: 10..20,
            contents: CompletedContents::Entries(tx_counts.to_vec()),
        }
    }

    fn summary(
        next_index: u32,
        entries_returned: usize,
        stop_reason: Option<PartialDecoderStop>,
    ) -> Option<DecoderSummary> {
        Some(DecoderSummary {
            next_index,
            entries_returned,
            stop_reason,
        })
    }

    fn classified(outcome: &'static str, txs: &[(&'static str, u64)]) -> DataSetClassification {
        DataSetClassification {
            outcome,
            txs: txs.to_vec(),
        }
    }

    #[test]
    fn test_classify_completed_data_set_outcomes() {
        let all_cached = |_| true;
        let none_cached = |_| false;
        // Nothing cached before the call wins over a decoder created, with
        // nothing appended, when the previous data set's last shred arrived.
        assert_eq!(
            classify_completed_data_set(&entries_set(&[2, 3]), summary(10, 0, None), none_cached),
            classified("same_call", &[("same_call", 5)])
        );
        assert_eq!(
            classify_completed_data_set(&entries_set(&[2, 3]), None, all_cached),
            classified("no_decoder", &[("no_decoder", 5)])
        );
        assert_eq!(
            classify_completed_data_set(
                &entries_set(&[0, 4, 1]),
                summary(11, 0, Some(PartialDecoderStop::Tick)),
                all_cached
            ),
            classified(
                "first_entry_tick",
                &[("after_tick", 4), ("last_entry_after_tick", 1)]
            )
        );
        assert_eq!(
            classify_completed_data_set(
                &entries_set(&[7]),
                summary(13, 0, Some(PartialDecoderStop::LastEntry)),
                all_cached
            ),
            classified("single_entry", &[("last_entry", 7)])
        );
        assert_eq!(
            classify_completed_data_set(
                &entries_set(&[2, 3, 0]),
                summary(12, 0, Some(PartialDecoderStop::Chain)),
                all_cached
            ),
            classified("stopped", &[("stopped", 5)])
        );
        // Waiting at shred 12: a gap if a later shred had arrived, else an
        // incomplete entry.
        assert_eq!(
            classify_completed_data_set(&entries_set(&[2, 3, 0]), summary(12, 0, None), |index| {
                index == 15
            }),
            classified("gap", &[("not_reached", 5)])
        );
        assert_eq!(
            classify_completed_data_set(&entries_set(&[2, 3, 0]), summary(12, 0, None), |index| {
                index < 12
            }),
            classified("incomplete_entry", &[("not_reached", 5)])
        );
        let marker = CompletedDataSet {
            indices: 10..11,
            contents: CompletedContents::BlockMarker,
        };
        assert_eq!(
            classify_completed_data_set(&marker, None, none_cached),
            classified("marker", &[])
        );
        let not_decoded = CompletedDataSet {
            indices: 10..11,
            contents: CompletedContents::NotDecoded,
        };
        assert_eq!(
            classify_completed_data_set(&not_decoded, None, all_cached),
            classified("not_decoded", &[])
        );
    }

    #[test]
    fn test_classify_completed_data_set_tx_classes() {
        let all_cached = |_| true;
        // Two entries early, one not reached, a tick, one after it, and the
        // last entry after the tick.
        assert_eq!(
            classify_completed_data_set(
                &entries_set(&[1, 2, 4, 0, 8, 16]),
                summary(15, 2, None),
                all_cached
            ),
            classified(
                "early",
                &[
                    ("early", 3),
                    ("not_reached", 4),
                    ("after_tick", 8),
                    ("last_entry_after_tick", 16)
                ]
            )
        );
        // No tick: everything but the last entry was sent.
        assert_eq!(
            classify_completed_data_set(
                &entries_set(&[1, 2, 4]),
                summary(15, 2, Some(PartialDecoderStop::LastEntry)),
                all_cached
            ),
            classified("early", &[("early", 3), ("last_entry", 4)])
        );
        // Stopped for another reason after an early chunk.
        assert_eq!(
            classify_completed_data_set(
                &entries_set(&[1, 2, 4]),
                summary(15, 1, Some(PartialDecoderStop::Invalid)),
                all_cached
            ),
            classified("early", &[("early", 1), ("stopped", 2), ("last_entry", 4)])
        );
        // A decoder claiming more entries than the data set has can't panic.
        assert_eq!(
            classify_completed_data_set(&entries_set(&[1, 2]), summary(15, 9, None), all_cached),
            classified("early", &[("early", 3)])
        );
        // Nor can an empty entry batch.
        assert_eq!(
            classify_completed_data_set(&entries_set(&[]), summary(15, 0, None), all_cached),
            classified("gap", &[])
        );
    }

    #[test]
    fn test_run_insert_sends_no_partial_entries_when_disabled() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path()).unwrap();
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank_forks = BankForks::new_rw_arc(Bank::new_for_tests(&genesis_config));
        let shreds = transfer_data_set_shreds(&Keypair::new());
        let (live_replay_sender, live_replay_receiver) = unbounded();
        let mut cache = ShredPayloadCache::new(4, false);
        for shred in &shreds[..shreds.len() - 1] {
            run_insert_for_tests(
                &blockstore,
                &bank_forks,
                std::slice::from_ref(shred),
                &mut cache,
                &live_replay_sender,
            );
        }
        assert_eq!(live_replay_receiver.try_iter().count(), 0);
        assert!(cache.partial.is_none());
    }

    #[test]
    fn test_no_partial_entries_for_a_slot_cleared_and_received_again() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path()).unwrap();
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank_forks = BankForks::new_rw_arc(Bank::new_for_tests(&genesis_config));
        let version_a = transfer_data_set_shreds(&Keypair::new());
        let version_b = transfer_data_set_shreds(&Keypair::new());
        let (live_replay_sender, live_replay_receiver) = unbounded();
        let mut cache = ShredPayloadCache::new(4, true);
        run_insert_for_tests(
            &blockstore,
            &bank_forks,
            &version_a[..3],
            &mut cache,
            &live_replay_sender,
        );
        live_replay_receiver.try_iter().for_each(drop);

        // Replay purges the slot (e.g. a duplicate block) and another version
        // arrives. `first_shred_timestamp` has millisecond resolution.
        blockstore.clear_unconfirmed_slot(1);
        thread::sleep(Duration::from_millis(5));
        run_insert_for_tests(
            &blockstore,
            &bank_forks,
            &version_b[..2],
            &mut cache,
            &live_replay_sender,
        );
        assert_eq!(live_replay_receiver.try_iter().count(), 0);
        let state = &cache.partial.as_ref().unwrap()[&1];
        assert!(state.disabled);
        assert!(state.decoders.is_empty());
        // Only the new version's shreds stay cached.
        let cached = &cache.payloads[&1];
        assert_eq!(cached.len(), 2);
        for shred in &version_b[..2] {
            assert_eq!(cached[&shred.index()].0, *shred.payload());
        }
    }

    #[test]
    fn test_run_check_duplicate() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank_forks = BankForks::new_rw_arc(Bank::new_for_tests(&genesis_config));
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());
        let (sender, receiver) = bounded(1024);
        let (duplicate_slot_sender, duplicate_slot_receiver) = bounded(1024);
        let (shreds, _) = make_many_slot_entries(5, 5, 10);
        blockstore.insert_shreds(shreds.clone(), false).unwrap();
        let duplicate_index = 0;
        let original_shred = shreds[duplicate_index].clone();
        let duplicate_shred = {
            let (mut shreds, _) = make_many_slot_entries(5, 1, 10);
            shreds.swap_remove(duplicate_index)
        };
        assert_eq!(duplicate_shred.slot(), shreds[0].slot());
        let duplicate_shred_slot = duplicate_shred.slot();
        sender
            .send(PossibleDuplicateShred::Exists(duplicate_shred.clone()))
            .unwrap();
        assert!(!blockstore.has_duplicate_shreds_in_slot(duplicate_shred_slot));
        let keypair = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), timestamp());
        let cluster_info = ClusterInfo::new(
            contact_info,
            Arc::new(keypair),
            SocketAddrSpace::Unspecified,
        );
        run_check_duplicate(
            &cluster_info,
            &blockstore,
            &receiver,
            &duplicate_slot_sender,
            &bank_forks,
        )
        .unwrap();

        // Make sure the correct duplicate proof was stored
        let duplicate_proof = blockstore.get_duplicate_slot(duplicate_shred_slot).unwrap();
        assert_eq!(duplicate_proof.shred1, *original_shred.payload());
        assert_eq!(duplicate_proof.shred2, *duplicate_shred.payload());

        // Make sure a duplicate signal was sent
        assert_eq!(
            duplicate_slot_receiver.try_recv().unwrap(),
            duplicate_shred_slot
        );
    }

    #[test]
    fn test_store_duplicate_shreds_same_batch() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());
        let (duplicate_shred_sender, duplicate_shred_receiver) = bounded(1024);
        let (duplicate_slot_sender, duplicate_slot_receiver) = bounded(1024);
        let exit = Arc::new(AtomicBool::new(false));
        let keypair = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), timestamp());
        let cluster_info = Arc::new(ClusterInfo::new(
            contact_info,
            Arc::new(keypair),
            SocketAddrSpace::Unspecified,
        ));
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank_forks = BankForks::new_rw_arc(Bank::new_for_tests(&genesis_config));

        // Start duplicate thread receiving and inserting duplicates
        let t_check_duplicate = WindowService::start_check_duplicate_thread(
            cluster_info,
            exit.clone(),
            blockstore.clone(),
            duplicate_shred_receiver,
            duplicate_slot_sender,
            bank_forks.clone(),
        );

        let handle_duplicate = |shred| {
            let _ = duplicate_shred_sender.send(shred);
        };
        let num_trials = 100;
        for slot in 0..num_trials {
            let (shreds, _) = make_many_slot_entries(slot, 1, 10);
            let duplicate_index = 0;
            let original_shred = shreds[duplicate_index].clone();
            let duplicate_shred = {
                let (mut shreds, _) = make_many_slot_entries(slot, 1, 10);
                shreds.swap_remove(duplicate_index)
            };
            assert_eq!(duplicate_shred.slot(), slot);
            // Simulate storing both duplicate shreds in the same batch
            let shreds = [&original_shred, &duplicate_shred]
                .into_iter()
                .map(|shred| (Cow::Borrowed(shred), /*is_repaired:*/ false));
            let (dummy_retransmit_sender, _) = EvictingSender::new_bounded(0);
            blockstore
                .insert_shreds_handle_duplicate(
                    shreds,
                    false, // is_trusted
                    &mut ShredRecoveryContext::new(
                        ReedSolomonCache::default(),
                        dummy_retransmit_sender,
                        bank_forks.read().unwrap().root_bank(),
                        0, // shred_version
                    ),
                    &handle_duplicate,
                    &mut BlockstoreInsertionMetrics::default(),
                )
                .unwrap();

            // Make sure a duplicate signal was sent
            assert_eq!(
                duplicate_slot_receiver
                    .recv_timeout(Duration::from_millis(5_000))
                    .unwrap(),
                slot
            );

            // Make sure the correct duplicate proof was stored
            let duplicate_proof = blockstore.get_duplicate_slot(slot).unwrap();
            assert_eq!(duplicate_proof.shred1, *original_shred.payload());
            assert_eq!(duplicate_proof.shred2, *duplicate_shred.payload());
        }
        exit.store(true, Ordering::Relaxed);
        t_check_duplicate.join().unwrap();
    }
}
