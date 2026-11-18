//! The live TVU replay path's ONLY source of entries.
//!
//! `window_service::run_insert` decodes each newly-completed data set from
//! the shred payloads it already holds in memory and sends it to `ReplayStage`
//! over a [`LiveReplaySender`] channel -- *before* `Blockstore` performs the
//! RocksDB write for that same insertion (see the `before_write` hook on
//! `Blockstore::insert_shreds_at_location_handle_duplicate`). The channel
//! send is also the wake-up: `ReplayStage` selects on the receiver.
//!
//! `ReplayStage` owns a [`LiveReplayEntries`], drains the channel into it, and
//! `confirm_slot`'s live branch reads *exclusively* from it via
//! [`LiveReplayEntries::drain_ready`] -- there is no RocksDB fallback.
//!
//! Bulk/offline replay (startup catch-up, ledger-tool) never uses this; it
//! has no `window_service` feeding it and keeps reading the persisted
//! blockstore.
//!
//! # Ordering and keying
//!
//! Data sets can complete out of order within a slot (e.g. erasure recovery),
//! so entries are keyed by `(slot, start_index)` and `drain_ready` walks the
//! contiguous chain from the caller's `start_index` itself, stopping at the
//! first gap or right after the `slot_full` data set.
//!
//! # Partial data sets
//!
//! With `AGAVE_PARTIAL_DATA_SET_REPLAY=1`, `window_service` also sends the
//! leading entries of a data set whose later shreds haven't arrived yet
//! ([`LiveReplayMessage::PartialEntries`], decoded by
//! [`PartialDataSetDecoder`]). `drain_ready` hands them to replay only for the
//! data set right after the contiguous chain, only if they continue exactly
//! where replay stopped in that data set, and only if they were decoded from
//! the slot's current version in the blockstore (see `generation`). Replay
//! later checks that the completed data set starts with exactly those entries
//! (`blockstore_processor::confirm_slot`).
//!
//! # Cleanup
//!
//! Consumed entries are removed as they're drained. Entries for slots replay
//! never follows are dropped by [`LiveReplayEntries::prune_through`], called
//! with the root slot as it advances.

use {
    crate::shred::{ShredFlags, wire},
    agave_transaction_view::transaction_view::UnsanitizedTransactionView,
    bytes::Bytes,
    crossbeam_channel::{Receiver, Sender},
    dashmap::{DashMap, DashSet, mapref::entry::Entry as DashEntry},
    solana_clock::Slot,
    solana_entry::{
        block_component::ParsedBlockComponent,
        block_component_parser::{
            ComponentHeader, EntryHeader, parse_component_header, parse_entry_header_at,
            parse_transaction_at,
        },
        entry::EntryView,
    },
    solana_hash::Hash,
    std::{mem, mem::size_of, ops::Range},
};

/// What `window_service` sends to replay over the live channel.
#[allow(clippy::large_enum_variant)]
pub enum LiveReplayMessage {
    /// One decoded, completed data set.
    Entries {
        slot: Slot,
        indices: Range<u32>,
        component: ParsedBlockComponent,
        slot_full: bool,
    },
    /// Leading entries of the data set starting at shred `start` that is not
    /// complete yet: its entries `first_entry..first_entry + entries.len()`.
    /// `generation` is the slot's `SlotMeta::first_shred_timestamp` when they
    /// were decoded; it changes when the slot is cleared and received again,
    /// so entries of a discarded version are never replayed.
    PartialEntries {
        slot: Slot,
        start: u32,
        generation: u64,
        first_entry: usize,
        entries: Vec<EntryView<Bytes>>,
    },
    /// `window_service` could not assemble this slot's next data set from
    /// memory (e.g. some of its shreds were persisted before a restart, or
    /// aged out of its payload cache). Replay must read the rest of this
    /// slot from the persisted blockstore. Never marks anything dead:
    /// whether the persisted data is actually bad is for the normal
    /// blockstore read path to decide.
    ReplayFromBlockstore { slot: Slot },
}

pub type LiveReplaySender = Sender<LiveReplayMessage>;
pub type LiveReplayReceiver = Receiver<LiveReplayMessage>;

struct Queued {
    indices: Range<u32>,
    component: ParsedBlockComponent,
    slot_full: bool,
}

/// Leading entries of one incomplete data set, `entries[0]` being the data
/// set's entry number `base`.
struct PartialQueued {
    generation: u64,
    base: usize,
    entries: Vec<EntryView<Bytes>>,
}

/// What [`LiveReplayEntries::drain_ready`] found for replay.
#[derive(Default)]
pub struct ReadyEntries {
    /// Completed data sets, in order, starting at the requested shred index.
    pub components: Vec<ParsedBlockComponent>,
    /// Shred index range of each completed data set.
    pub ranges: Vec<Range<u32>>,
    /// Whether the last completed data set ends the slot.
    pub slot_full: bool,
    /// Leading entries of the incomplete data set that follows `components`
    /// (never when `slot_full`), continuing where replay stopped in it.
    pub partial_entries: Vec<EntryView<Bytes>>,
}

/// Shared by reference across replay's parallel per-bank workers (hence
/// `DashMap`), owned by `ReplayStage`.
#[derive(Default)]
pub struct LiveReplayEntries {
    queued: DashMap<(Slot, u32), Queued>,
    /// Keyed like `queued`, by the incomplete data set's start index.
    partial: DashMap<(Slot, u32), PartialQueued>,
    /// Slots whose replay was restarted by a dump/purge while their shreds
    /// were left in the blockstore (see `replay_stage::purge_unconfirmed_slot`).
    /// Their data sets were already consumed from `queued` and will never be
    /// sent again, so they must be replayed from the persisted blockstore.
    /// A rare recovery path, not the steady-state one.
    replay_from_blockstore: DashSet<Slot>,
}

impl LiveReplayEntries {
    pub fn push(&self, message: LiveReplayMessage) {
        match message {
            LiveReplayMessage::Entries {
                slot,
                indices,
                component,
                slot_full,
            } => {
                // The completed data set supersedes its leading entries.
                self.partial.remove(&(slot, indices.start));
                self.queued.insert(
                    (slot, indices.start),
                    Queued {
                        indices,
                        component,
                        slot_full,
                    },
                );
            }
            LiveReplayMessage::PartialEntries {
                slot,
                start,
                generation,
                first_entry,
                entries,
            } => self.push_partial(slot, start, generation, first_entry, entries),
            LiveReplayMessage::ReplayFromBlockstore { slot } => {
                self.remove_slot(slot);
                self.mark_replay_from_blockstore(slot);
            }
        }
    }

    /// Queues a chunk only if it continues the queued entries of the same
    /// generation exactly, or starts a data set (`first_entry == 0`) of a new
    /// generation. Anything else is a duplicate or comes from a discarded
    /// version, and is dropped.
    fn push_partial(
        &self,
        slot: Slot,
        start: u32,
        generation: u64,
        first_entry: usize,
        entries: Vec<EntryView<Bytes>>,
    ) {
        let accepted = match self.partial.entry((slot, start)) {
            DashEntry::Occupied(mut occupied) => {
                let queued = occupied.get_mut();
                if queued.generation == generation
                    && first_entry == queued.base.saturating_add(queued.entries.len())
                {
                    queued.entries.extend(entries);
                    true
                } else if queued.generation != generation && first_entry == 0 {
                    *queued = PartialQueued {
                        generation,
                        base: 0,
                        entries,
                    };
                    true
                } else {
                    false
                }
            }
            DashEntry::Vacant(vacant) => {
                if first_entry == 0 {
                    vacant.insert(PartialQueued {
                        generation,
                        base: 0,
                        entries,
                    });
                    true
                } else {
                    false
                }
            }
        };
        if !accepted {
            solana_metrics::pipeline_metrics::PARTIAL_DATA_SET_TOTAL
                .with_label_values(&["chunk_stale"])
                .inc();
        }
    }

    /// Moves everything currently sitting in `receiver` into this structure.
    pub fn absorb(&self, receiver: &LiveReplayReceiver) {
        for message in receiver.try_iter() {
            self.push(message);
        }
    }

    /// `confirm_slot`'s live-replay source of entries. Walks the chain of
    /// contiguous completed ranges for `slot` starting at `start_index`,
    /// removing each as consumed. Then, if the slot isn't full, takes the
    /// queued leading entries of the next (incomplete) data set, but only if
    /// they start at that data set's entry number `partial_consumed` (when it
    /// is the data set at `start_index`; 0 otherwise) and were decoded from
    /// the slot's current version: `current_generation` must return their
    /// `generation`. It is only called when such entries are queued.
    ///
    /// Returns nothing when nothing new is queued, which callers must treat
    /// as "polled too early", not an error.
    pub fn drain_ready(
        &self,
        slot: Slot,
        start_index: u64,
        partial_consumed: usize,
        current_generation: impl FnOnce() -> Option<u64>,
    ) -> ReadyEntries {
        let mut ready = ReadyEntries::default();
        let mut next_start = start_index as u32;
        while let Some((_, queued)) = self.queued.remove(&(slot, next_start)) {
            next_start = queued.indices.end;
            ready.slot_full = queued.slot_full;
            ready.ranges.push(queued.indices);
            ready.components.push(queued.component);
            if ready.slot_full {
                break;
            }
        }
        solana_metrics::pipeline_metrics::LIVE_REPLAY_ENTRIES_TOTAL
            .with_label_values(&[if ready.components.is_empty() {
                "not_ready"
            } else {
                "ready"
            }])
            .inc();

        if !ready.slot_full && self.partial.contains_key(&(slot, next_start)) {
            let consumed = if next_start == start_index as u32 {
                partial_consumed
            } else {
                0
            };
            let generation = current_generation();
            if let Some(mut queued) = self.partial.get_mut(&(slot, next_start))
                && Some(queued.generation) == generation
                && queued.base == consumed
                && !queued.entries.is_empty()
            {
                // The key stays, with `base` advanced, so later chunks for
                // this data set keep lining up.
                ready.partial_entries = mem::take(&mut queued.entries);
                queued.base = queued.base.saturating_add(ready.partial_entries.len());
            }
        }
        ready
    }

    /// Drops everything queued for slots `<= root`.
    pub fn prune_through(&self, root: Slot) {
        self.queued.retain(|(slot, _), _| *slot > root);
        self.partial.retain(|(slot, _), _| *slot > root);
        self.replay_from_blockstore.retain(|slot| *slot > root);
    }

    /// Drops everything queued for `slot`. Called when a slot is purged: its
    /// queued entries belong to a version of the block that is being
    /// discarded and must not mix with the repaired one.
    pub fn remove_slot(&self, slot: Slot) {
        self.queued
            .retain(|(queued_slot, _), _| *queued_slot != slot);
        self.partial
            .retain(|(queued_slot, _), _| *queued_slot != slot);
    }

    /// See `replay_from_blockstore`'s docs.
    pub fn mark_replay_from_blockstore(&self, slot: Slot) {
        self.replay_from_blockstore.insert(slot);
    }

    pub fn replays_from_blockstore(&self, slot: Slot) -> bool {
        self.replay_from_blockstore.contains(&slot)
    }
}

/// Unparsed bytes a [`PartialDataSetDecoder`] may hold without completing a
/// transaction before it gives up. Only bounds work on adversarial data;
/// giving up just means the data set replays whole.
const MAX_UNPARSED_BYTES: usize = 64 * 1024;
/// Transactions a decoder preallocates room for per entry.
const MAX_PREALLOCATED_TXS: usize = 64;
const ENTRY_COUNT_SIZE: usize = size_of::<u64>();
const ENTRY_HEADER_SIZE: usize = 2 * size_of::<u64>() + size_of::<Hash>();

/// Decodes the leading entries of a data set from its shreds as they arrive,
/// in index order from the data set's first shred, for partial data-set
/// replay.
///
/// Uses the same header, entry-header and transaction parsing as the
/// completed data set's decode (`block_component_parser::parse`), on the
/// same leading bytes, so every entry it returns is exactly the completed
/// data set's entry at that position. It returns only complete entries that
/// are not a tick and not the data set's last entry, and stops for good at:
/// - a shred that doesn't continue the run: wrong index, not a data shred,
///   the data set's last shred (`DATA_COMPLETE_SHRED`), a different merkle
///   root within an FEC set, or an FEC set whose chained merkle root isn't
///   the previous FEC set's root (shreds of two versions of a block are never
///   combined);
/// - a block marker, an invalid header, a tick, the last entry, or more than
///   `MAX_UNPARSED_BYTES` without a complete transaction.
///
/// Parsing resumes where it stopped, per transaction, so each byte is parsed
/// once.
pub struct PartialDataSetDecoder {
    next_index: u32,
    /// FEC set index and merkle root of the last appended shred.
    last_merkle_root: Option<(u32, Hash)>,
    /// Appended data not yet consumed by a parsed header or transaction.
    unparsed: Vec<u8>,
    entry_count: Option<usize>,
    /// The entry being parsed: its header and the transactions parsed so far.
    current: Option<(EntryHeader, Vec<UnsanitizedTransactionView<Bytes>>)>,
    entries_parsed: usize,
    /// Parsed entries not yet returned.
    ready: Vec<EntryView<Bytes>>,
    entries_returned: usize,
    /// Data was appended since the last parse; parsing again without new
    /// data could not make progress.
    appended_since_parse: bool,
    stopped: bool,
}

impl PartialDataSetDecoder {
    pub fn new(start_index: u32) -> Self {
        Self {
            next_index: start_index,
            last_merkle_root: None,
            unparsed: Vec::new(),
            entry_count: None,
            current: None,
            entries_parsed: 0,
            ready: Vec::new(),
            entries_returned: 0,
            appended_since_parse: false,
            stopped: false,
        }
    }

    /// Index of the next shred `append` accepts.
    pub fn next_index(&self) -> u32 {
        self.next_index
    }

    /// Whether this decoder will return no more entries.
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    /// Appends the data of shred `next_index()`. Returns false, and stops the
    /// decoder, if `payload` doesn't continue the run (see the type docs).
    pub fn append(&mut self, payload: &[u8]) -> bool {
        if self.stopped {
            return false;
        }
        let Some((fec_set_index, merkle_root, data)) = self.checked_data(payload) else {
            self.stop();
            return false;
        };
        self.unparsed.extend_from_slice(data);
        self.appended_since_parse = true;
        self.last_merkle_root = Some((fec_set_index, merkle_root));
        self.next_index = self.next_index.saturating_add(1);
        true
    }

    /// Parses what has been appended and returns the entries not returned
    /// before, with the data-set entry number of the first one.
    pub fn take_new_entries(&mut self) -> Option<(usize, Vec<EntryView<Bytes>>)> {
        self.parse_available();
        if self.ready.is_empty() {
            return None;
        }
        let first_entry = self.entries_returned;
        let entries = mem::take(&mut self.ready);
        self.entries_returned = self.entries_returned.saturating_add(entries.len());
        Some((first_entry, entries))
    }

    fn stop(&mut self) {
        self.stopped = true;
        self.unparsed = Vec::new();
        self.current = None;
    }

    fn checked_data<'a>(&self, payload: &'a [u8]) -> Option<(u32, Hash, &'a [u8])> {
        if wire::get_index(payload)? != self.next_index {
            return None;
        }
        // Errs for coding shreds.
        let flags = wire::get_flags(payload).ok()?;
        if flags.contains(ShredFlags::DATA_COMPLETE_SHRED) {
            return None;
        }
        let fec_set_index = wire::get_fec_set_index(payload)?;
        let merkle_root = wire::get_merkle_root(payload)?;
        if let Some((last_fec_set_index, last_merkle_root)) = self.last_merkle_root {
            let continues = if last_fec_set_index == fec_set_index {
                // Same FEC set: same merkle root.
                merkle_root == last_merkle_root
            } else {
                // Next FEC set: starts here and chains to the previous root.
                fec_set_index == self.next_index
                    && wire::get_chained_merkle_root(payload)? == last_merkle_root
            };
            if !continues {
                return None;
            }
        }
        let data = wire::get_data(payload).ok()?;
        Some((fec_set_index, merkle_root, data))
    }

    fn parse_available(&mut self) {
        if self.stopped || !mem::take(&mut self.appended_since_parse) {
            return;
        }
        // Views keep slices of this copy alive; only the not yet consumed
        // tail is copied.
        let bytes = Bytes::copy_from_slice(&self.unparsed);
        let mut offset = 0;
        let entry_count = match self.entry_count {
            Some(entry_count) => entry_count,
            None => {
                if bytes.len() < ENTRY_COUNT_SIZE {
                    return;
                }
                match parse_component_header(&bytes) {
                    Ok(ComponentHeader::EntryBatch {
                        entry_count,
                        entries_offset,
                    }) => {
                        self.entry_count = Some(entry_count);
                        offset = entries_offset;
                        entry_count
                    }
                    // Block markers are never replayed early.
                    Ok(ComponentHeader::BlockMarker) | Err(_) => {
                        self.stop();
                        return;
                    }
                }
            }
        };

        // The data set's last entry is never returned: replay must see at
        // least one entry of every data set with the completed data set.
        'entries: while self.entries_parsed.saturating_add(1) < entry_count {
            if self.current.is_none() {
                match parse_entry_header_at(&bytes, offset, self.entries_parsed) {
                    Ok((header, transactions_offset)) => {
                        if header.tx_count == 0 {
                            // A tick: never replayed early, nor anything after it.
                            self.stop();
                            return;
                        }
                        offset = transactions_offset;
                        // tx_count is untrusted and the entry may stay
                        // incomplete for long, so don't preallocate for it.
                        self.current = Some((
                            header,
                            Vec::with_capacity(header.tx_count.min(MAX_PREALLOCATED_TXS)),
                        ));
                    }
                    Err(_) => {
                        if bytes.len().saturating_sub(offset) >= ENTRY_HEADER_SIZE {
                            // A complete but invalid header.
                            self.stop();
                            return;
                        }
                        break 'entries;
                    }
                }
            }
            let Some((header, transactions)) = self.current.as_mut() else {
                break 'entries;
            };
            while transactions.len() < header.tx_count {
                match parse_transaction_at(&bytes, offset, self.entries_parsed, transactions.len())
                {
                    Ok((view, next_offset)) => {
                        transactions.push(view);
                        offset = next_offset;
                    }
                    // Not all of it has arrived yet (or it is invalid, which
                    // MAX_UNPARSED_BYTES bounds).
                    Err(_) => break 'entries,
                }
            }
            if let Some((header, transactions)) = self.current.take() {
                self.ready.push(EntryView {
                    num_hashes: header.num_hashes,
                    hash: header.hash,
                    transactions,
                });
                self.entries_parsed = self.entries_parsed.saturating_add(1);
            }
        }
        if self.entries_parsed.saturating_add(1) >= entry_count {
            self.stop();
            return;
        }
        self.unparsed.drain(..offset);
        if self.unparsed.len() > MAX_UNPARSED_BYTES {
            self.stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::shred::{ProcessShredsStats, ReedSolomonCache, Shred, Shredder},
        crossbeam_channel::unbounded,
        solana_entry::entry::Entry,
        solana_keypair::Keypair,
        solana_pubkey::Pubkey,
        solana_system_transaction as system_transaction,
    };

    fn entry_batch(num: u64) -> ParsedBlockComponent {
        let component = solana_entry::block_component::BlockComponent::EntryBatch(vec![
            solana_entry::entry::Entry {
                num_hashes: num,
                hash: solana_hash::Hash::default(),
                transactions: vec![],
            },
        ]);
        solana_entry::block_component_parser::parse(wincode::serialize(&component).unwrap())
            .unwrap()
    }

    /// `ParsedBlockComponent` has no `PartialEq`; tests identify each queued
    /// entry batch by its single entry's `num_hashes`.
    fn nums(components: &[ParsedBlockComponent]) -> Vec<u64> {
        components
            .iter()
            .map(|component| match component {
                ParsedBlockComponent::EntryBatch(entries) => entries[0].num_hashes,
                ParsedBlockComponent::BlockMarker(_) => panic!("expected EntryBatch"),
            })
            .collect()
    }

    fn message(slot: Slot, indices: Range<u32>, num: u64, slot_full: bool) -> LiveReplayMessage {
        LiveReplayMessage::Entries {
            slot,
            indices,
            component: entry_batch(num),
            slot_full,
        }
    }

    /// Entry views identified by `num_hashes`.
    fn views(nums: &[u64]) -> Vec<EntryView<Bytes>> {
        nums.iter()
            .map(|num| match entry_batch(*num) {
                ParsedBlockComponent::EntryBatch(mut entries) => entries.remove(0),
                ParsedBlockComponent::BlockMarker(_) => unreachable!(),
            })
            .collect()
    }

    fn view_nums(entries: &[EntryView<Bytes>]) -> Vec<u64> {
        entries.iter().map(|entry| entry.num_hashes).collect()
    }

    fn partial(
        slot: Slot,
        start: u32,
        generation: u64,
        first_entry: usize,
        nums: &[u64],
    ) -> LiveReplayMessage {
        LiveReplayMessage::PartialEntries {
            slot,
            start,
            generation,
            first_entry,
            entries: views(nums),
        }
    }

    fn drain(live: &LiveReplayEntries, slot: Slot, start_index: u64) -> ReadyEntries {
        live.drain_ready(slot, start_index, 0, || Some(1))
    }

    #[test]
    fn drains_contiguous_chain_in_one_call() {
        let live = LiveReplayEntries::default();
        live.push(message(1, 0..4, 1, false));
        live.push(message(1, 4..7, 2, true));

        let ready = drain(&live, 1, 0);
        assert_eq!(nums(&ready.components), vec![1, 2]);
        assert_eq!(ready.ranges, vec![0..4, 4..7]);
        assert!(ready.slot_full);

        let ready = drain(&live, 1, 0);
        assert!(ready.components.is_empty() && ready.ranges.is_empty() && !ready.slot_full);
    }

    #[test]
    fn stops_at_first_gap_and_leaves_it_queued() {
        let live = LiveReplayEntries::default();
        live.push(message(2, 4..7, 2, false));
        assert!(drain(&live, 2, 0).components.is_empty());

        live.push(message(2, 0..4, 1, false));
        let ready = drain(&live, 2, 0);
        assert_eq!(nums(&ready.components), vec![1, 2]);
        assert_eq!(ready.ranges, vec![0..4, 4..7]);
    }

    #[test]
    fn nothing_queued_is_not_an_error() {
        let live = LiveReplayEntries::default();
        let ready = drain(&live, 3, 0);
        assert!(ready.components.is_empty() && ready.ranges.is_empty() && !ready.slot_full);
        assert!(ready.partial_entries.is_empty());
    }

    #[test]
    fn absorb_moves_channel_contents() {
        let live = LiveReplayEntries::default();
        let (sender, receiver) = unbounded();
        sender.send(message(4, 0..2, 1, true)).unwrap();
        live.absorb(&receiver);
        let ready = drain(&live, 4, 0);
        assert_eq!(nums(&ready.components), vec![1]);
        assert!(ready.slot_full);
    }

    #[test]
    fn replay_from_blockstore_message_clears_queue_and_flags_slot() {
        let live = LiveReplayEntries::default();
        live.push(message(6, 4..7, 2, false));
        live.push(partial(6, 7, 1, 0, &[9]));
        live.push(LiveReplayMessage::ReplayFromBlockstore { slot: 6 });
        assert!(live.replays_from_blockstore(6));
        assert!(drain(&live, 6, 4).components.is_empty());
        assert!(drain(&live, 6, 7).partial_entries.is_empty());
    }

    #[test]
    fn remove_slot_drops_only_that_slot() {
        let live = LiveReplayEntries::default();
        live.push(message(7, 0..2, 1, true));
        live.push(message(8, 0..2, 2, true));
        live.push(partial(7, 2, 1, 0, &[3]));
        live.push(partial(8, 2, 1, 0, &[4]));
        live.remove_slot(7);
        assert!(drain(&live, 7, 0).components.is_empty());
        assert!(drain(&live, 7, 2).partial_entries.is_empty());
        assert_eq!(nums(&drain(&live, 8, 0).components), vec![2]);
        assert_eq!(view_nums(&drain(&live, 8, 2).partial_entries), vec![4]);
    }

    #[test]
    fn prune_through_drops_old_slots_only() {
        let live = LiveReplayEntries::default();
        live.push(message(5, 0..2, 1, true));
        live.push(message(9, 0..2, 2, true));
        live.push(partial(5, 2, 1, 0, &[3]));
        live.push(partial(9, 2, 1, 0, &[4]));
        live.prune_through(5);
        assert!(drain(&live, 5, 0).components.is_empty());
        assert!(drain(&live, 5, 2).partial_entries.is_empty());
        assert_eq!(nums(&drain(&live, 9, 0).components), vec![2]);
        assert_eq!(view_nums(&drain(&live, 9, 2).partial_entries), vec![4]);
    }

    #[test]
    fn partial_chunks_drain_incrementally_between_pushes() {
        let live = LiveReplayEntries::default();
        live.push(partial(10, 0, 1, 0, &[1, 2]));
        let ready = live.drain_ready(10, 0, 0, || Some(1));
        assert!(ready.components.is_empty());
        assert_eq!(view_nums(&ready.partial_entries), vec![1, 2]);

        // The next chunk continues where the last drain stopped.
        live.push(partial(10, 0, 1, 2, &[3]));
        assert_eq!(
            view_nums(&live.drain_ready(10, 0, 2, || Some(1)).partial_entries),
            vec![3]
        );
        // A duplicate or a gap is dropped.
        live.push(partial(10, 0, 1, 2, &[3]));
        live.push(partial(10, 0, 1, 5, &[6]));
        assert!(
            live.drain_ready(10, 0, 3, || Some(1))
                .partial_entries
                .is_empty()
        );
        live.push(partial(10, 0, 1, 3, &[4]));
        assert_eq!(
            view_nums(&live.drain_ready(10, 0, 3, || Some(1)).partial_entries),
            vec![4]
        );
    }

    #[test]
    fn partial_entries_must_continue_where_replay_stopped() {
        let live = LiveReplayEntries::default();
        live.push(partial(11, 0, 1, 0, &[1, 2]));
        assert_eq!(
            view_nums(&live.drain_ready(11, 0, 0, || Some(1)).partial_entries),
            vec![1, 2]
        );
        live.push(partial(11, 0, 1, 2, &[3]));
        // A replay that ran nothing of this data set although entries 0..2
        // were already taken (e.g. a bank restarted from scratch) gets nothing.
        assert!(
            live.drain_ready(11, 0, 0, || Some(1))
                .partial_entries
                .is_empty()
        );
        // The replay that ran entries 0..2 gets entry 2.
        assert_eq!(
            view_nums(&live.drain_ready(11, 0, 2, || Some(1)).partial_entries),
            vec![3]
        );
    }

    #[test]
    fn partial_entries_of_another_generation_are_not_replayed() {
        let live = LiveReplayEntries::default();
        // A chunk decoded before the slot was cleared, still in flight when
        // replay purged the slot, is absorbed after the purge.
        live.push(partial(12, 0, 1, 0, &[1]));
        live.remove_slot(12);
        live.push(partial(12, 0, 1, 0, &[1]));
        // The blockstore now holds generation 2: the old chunk is not replayed.
        assert!(
            live.drain_ready(12, 0, 0, || Some(2))
                .partial_entries
                .is_empty()
        );
        // The new version's first chunk replaces it.
        live.push(partial(12, 0, 2, 0, &[7]));
        assert_eq!(
            view_nums(&live.drain_ready(12, 0, 0, || Some(2)).partial_entries),
            vec![7]
        );
        // Nor when the slot has no shreds at all.
        live.push(partial(12, 0, 2, 1, &[8]));
        assert!(
            live.drain_ready(12, 0, 1, || None)
                .partial_entries
                .is_empty()
        );
    }

    #[test]
    fn partial_entries_follow_the_completed_chain() {
        let live = LiveReplayEntries::default();
        live.push(message(13, 0..4, 1, false));
        live.push(partial(13, 4, 1, 0, &[5, 6]));
        // Data set 0..4 completed, so replay hasn't consumed anything of the
        // data set at 4, whatever it consumed of the one at 0.
        let ready = live.drain_ready(13, 0, 3, || Some(1));
        assert_eq!(nums(&ready.components), vec![1]);
        assert_eq!(view_nums(&ready.partial_entries), vec![5, 6]);
    }

    #[test]
    fn completed_data_set_replaces_its_partial_entries() {
        let live = LiveReplayEntries::default();
        live.push(partial(14, 0, 1, 0, &[1]));
        live.push(message(14, 0..3, 9, false));
        let ready = live.drain_ready(14, 0, 0, || Some(1));
        assert_eq!(nums(&ready.components), vec![9]);
        assert!(ready.partial_entries.is_empty());
    }

    #[test]
    fn no_partial_entries_after_a_full_slot() {
        let live = LiveReplayEntries::default();
        live.push(message(15, 0..3, 1, true));
        live.push(partial(15, 3, 1, 0, &[2]));
        let ready = live.drain_ready(15, 0, 0, || Some(1));
        assert!(ready.slot_full);
        assert!(ready.partial_entries.is_empty());
    }

    fn test_entries(num_entries: u64, txs_per_entry: u64) -> Vec<Entry> {
        let mut hash = Hash::new_unique();
        let mut entries: Vec<Entry> = (0..num_entries)
            .map(|i| {
                let transactions = (0..txs_per_entry)
                    .map(|j| {
                        system_transaction::transfer(
                            &Keypair::new(),
                            &Pubkey::new_unique(),
                            i * 100 + j + 1,
                            Hash::default(),
                        )
                    })
                    .collect();
                let entry = solana_entry::entry::next_entry(&hash, 1, transactions);
                hash = entry.hash;
                entry
            })
            .collect();
        entries.push(solana_entry::entry::next_entry(&hash, 1, vec![]));
        entries
    }

    fn data_shreds(entries: &[Entry], keypair: &Keypair) -> Vec<Shred> {
        Shredder::new(1, 0, 0, 0)
            .unwrap()
            .make_merkle_shreds_from_entries(
                keypair,
                entries,
                false, // is_last_in_slot
                Hash::new_unique(),
                0,
                0,
                &ReedSolomonCache::default(),
                &mut ProcessShredsStats::default(),
            )
            .into_iter()
            .filter(Shred::is_data)
            .collect()
    }

    type EntryBytes = (u64, Hash, Vec<Vec<u8>>);

    fn entry_bytes(entries: &[EntryView<Bytes>]) -> Vec<EntryBytes> {
        entries
            .iter()
            .map(|entry| {
                (
                    entry.num_hashes,
                    entry.hash,
                    entry
                        .transactions
                        .iter()
                        .map(|tx| tx.data().to_vec())
                        .collect(),
                )
            })
            .collect()
    }

    #[test]
    fn decoder_returns_exactly_the_completed_data_sets_leading_entries() {
        let keypair = Keypair::new();
        // Entries of ~10 transfers each span several shreds, so transactions
        // are split across shred boundaries.
        let entries = test_entries(8, 10);
        let shreds = data_shreds(&entries, &keypair);
        assert!(shreds.len() > 8);
        let payloads: Vec<_> = shreds.iter().map(|shred| shred.payload().clone()).collect();
        let ParsedBlockComponent::EntryBatch(full) =
            crate::blockstore::Blockstore::decode_completed_data_set_from_payloads(
                payloads.clone(),
            )
            .unwrap()
        else {
            panic!("expected EntryBatch");
        };
        let full = entry_bytes(&full);

        let mut decoder = PartialDataSetDecoder::new(0);
        let mut decoded = vec![];
        // Every shred but the last (DATA_COMPLETE) one, one at a time.
        for payload in &payloads[..payloads.len() - 1] {
            if !decoder.append(payload) {
                // Only once everything but the data set's last entry was
                // returned: the decoder has nothing more to send.
                assert!(decoder.is_stopped());
                assert_eq!(decoded.len(), full.len() - 1);
                break;
            }
            if let Some((first_entry, chunk)) = decoder.take_new_entries() {
                assert_eq!(first_entry, decoded.len());
                decoded.extend(entry_bytes(&chunk));
            }
        }
        // The completed data set's last shred is never part of a partial run.
        assert!(!decoder.append(payloads.last().unwrap()));
        assert!(decoder.is_stopped());

        assert!(!decoded.is_empty());
        // Never the data set's last entry (here also its only tick).
        assert!(decoded.len() < full.len());
        assert_eq!(decoded[..], full[..decoded.len()]);
    }

    #[test]
    fn decoder_returns_nothing_past_the_data_sets_last_entry_or_a_tick() {
        let keypair = Keypair::new();
        let mut entries = test_entries(3, 10);
        // Move the tick to the middle: entries after it are never returned.
        let tick = entries.pop().unwrap();
        entries.insert(1, tick);
        let shreds = data_shreds(&entries, &keypair);
        let mut decoder = PartialDataSetDecoder::new(0);
        let mut decoded = vec![];
        for shred in &shreds[..shreds.len() - 1] {
            decoder.append(shred.payload());
            if let Some((_, chunk)) = decoder.take_new_entries() {
                decoded.extend(entry_bytes(&chunk));
            }
        }
        assert!(decoded.len() <= 1);
        assert!(decoder.is_stopped());
    }

    #[test]
    fn decoder_stops_on_a_shred_from_another_version() {
        let entries = test_entries(8, 10);
        let version_a = data_shreds(&entries, &Keypair::new());
        let version_b = data_shreds(&entries, &Keypair::new());
        let mut decoder = PartialDataSetDecoder::new(0);
        assert!(decoder.append(version_a[0].payload()));
        // Same index and content but a different leader signature/merkle root.
        assert!(!decoder.append(version_b[1].payload()));
        assert!(decoder.is_stopped());
        assert!(decoder.take_new_entries().is_none());
    }

    #[test]
    fn decoder_stops_on_an_index_gap_or_a_coding_shred() {
        let entries = test_entries(8, 10);
        let keypair = Keypair::new();
        let shreds = data_shreds(&entries, &keypair);
        let mut decoder = PartialDataSetDecoder::new(0);
        assert!(!decoder.append(shreds[1].payload()));
        assert!(decoder.is_stopped());

        let coding = Shredder::new(1, 0, 0, 0)
            .unwrap()
            .make_merkle_shreds_from_entries(
                &keypair,
                &entries,
                false,
                Hash::new_unique(),
                0,
                0,
                &ReedSolomonCache::default(),
                &mut ProcessShredsStats::default(),
            )
            .into_iter()
            .find(|shred| !shred.is_data())
            .unwrap();
        let mut decoder = PartialDataSetDecoder::new(coding.index());
        assert!(!decoder.append(coding.payload()));
    }
}
