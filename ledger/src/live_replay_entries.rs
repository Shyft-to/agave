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
//! # Cleanup
//!
//! Consumed entries are removed as they're drained. Entries for slots replay
//! never follows are dropped by [`LiveReplayEntries::prune_through`], called
//! with the root slot as it advances.

use {
    crossbeam_channel::{Receiver, Sender},
    dashmap::DashMap,
    solana_clock::Slot,
    solana_entry::block_component::BlockComponent,
    std::ops::Range,
};

/// One decoded, completed data set on its way to replay.
pub struct LiveReplayMessage {
    pub slot: Slot,
    pub indices: Range<u32>,
    pub component: BlockComponent,
    pub slot_full: bool,
}

pub type LiveReplaySender = Sender<LiveReplayMessage>;
pub type LiveReplayReceiver = Receiver<LiveReplayMessage>;

struct Queued {
    indices: Range<u32>,
    component: BlockComponent,
    slot_full: bool,
}

/// Shared by reference across replay's parallel per-bank workers (hence
/// `DashMap`), owned by `ReplayStage`.
#[derive(Default)]
pub struct LiveReplayEntries {
    queued: DashMap<(Slot, u32), Queued>,
}

impl LiveReplayEntries {
    pub fn push(&self, message: LiveReplayMessage) {
        let LiveReplayMessage {
            slot,
            indices,
            component,
            slot_full,
        } = message;
        self.queued.insert(
            (slot, indices.start),
            Queued {
                indices,
                component,
                slot_full,
            },
        );
    }

    /// Moves everything currently sitting in `receiver` into this structure.
    pub fn absorb(&self, receiver: &LiveReplayReceiver) {
        for message in receiver.try_iter() {
            self.push(message);
        }
    }

    /// `confirm_slot`'s live-replay source of entries. Walks the chain of
    /// contiguous completed ranges for `slot` starting at `start_index`,
    /// removing each as consumed. Returns `(vec![], vec![], false)` when
    /// nothing new is queued, which callers must treat as "polled too
    /// early", not an error.
    pub fn drain_ready(
        &self,
        slot: Slot,
        start_index: u64,
    ) -> (Vec<BlockComponent>, Vec<Range<u32>>, bool) {
        let mut components = Vec::new();
        let mut ranges = Vec::new();
        let mut slot_full = false;
        let mut next_start = start_index as u32;
        while let Some((_, queued)) = self.queued.remove(&(slot, next_start)) {
            next_start = queued.indices.end;
            slot_full = queued.slot_full;
            ranges.push(queued.indices);
            components.push(queued.component);
            if slot_full {
                break;
            }
        }
        solana_metrics::pipeline_metrics::LIVE_REPLAY_ENTRIES_TOTAL
            .with_label_values(&[if components.is_empty() {
                "not_ready"
            } else {
                "ready"
            }])
            .inc();
        (components, ranges, slot_full)
    }

    /// Drops everything queued for slots `<= root`.
    pub fn prune_through(&self, root: Slot) {
        self.queued.retain(|(slot, _), _| *slot > root);
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crossbeam_channel::unbounded};

    fn entry_batch(num: u64) -> BlockComponent {
        BlockComponent::EntryBatch(vec![solana_entry::entry::Entry {
            num_hashes: num,
            hash: solana_hash::Hash::default(),
            transactions: vec![],
        }])
    }

    fn message(slot: Slot, indices: Range<u32>, num: u64, slot_full: bool) -> LiveReplayMessage {
        LiveReplayMessage {
            slot,
            indices,
            component: entry_batch(num),
            slot_full,
        }
    }

    #[test]
    fn drains_contiguous_chain_in_one_call() {
        let live = LiveReplayEntries::default();
        live.push(message(1, 0..4, 1, false));
        live.push(message(1, 4..7, 2, true));

        let (components, ranges, slot_full) = live.drain_ready(1, 0);
        assert_eq!(components, vec![entry_batch(1), entry_batch(2)]);
        assert_eq!(ranges, vec![0..4, 4..7]);
        assert!(slot_full);

        let (components, ranges, slot_full) = live.drain_ready(1, 0);
        assert!(components.is_empty() && ranges.is_empty() && !slot_full);
    }

    #[test]
    fn stops_at_first_gap_and_leaves_it_queued() {
        let live = LiveReplayEntries::default();
        live.push(message(2, 4..7, 2, false));
        let (components, _, _) = live.drain_ready(2, 0);
        assert!(components.is_empty());

        live.push(message(2, 0..4, 1, false));
        let (components, ranges, _) = live.drain_ready(2, 0);
        assert_eq!(components, vec![entry_batch(1), entry_batch(2)]);
        assert_eq!(ranges, vec![0..4, 4..7]);
    }

    #[test]
    fn nothing_queued_is_not_an_error() {
        let live = LiveReplayEntries::default();
        let (components, ranges, slot_full) = live.drain_ready(3, 0);
        assert!(components.is_empty() && ranges.is_empty() && !slot_full);
    }

    #[test]
    fn absorb_moves_channel_contents() {
        let live = LiveReplayEntries::default();
        let (sender, receiver) = unbounded();
        sender.send(message(4, 0..2, 1, true)).unwrap();
        live.absorb(&receiver);
        let (components, _, slot_full) = live.drain_ready(4, 0);
        assert_eq!(components, vec![entry_batch(1)]);
        assert!(slot_full);
    }

    #[test]
    fn prune_through_drops_old_slots_only() {
        let live = LiveReplayEntries::default();
        live.push(message(5, 0..2, 1, true));
        live.push(message(9, 0..2, 2, true));
        live.prune_through(5);
        assert!(live.drain_ready(5, 0).0.is_empty());
        assert_eq!(live.drain_ready(9, 0).0, vec![entry_batch(2)]);
    }
}
