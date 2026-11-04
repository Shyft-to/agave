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
    dashmap::{DashMap, DashSet},
    solana_clock::Slot,
    solana_entry::block_component::BlockComponent,
    std::ops::Range,
};

/// What `window_service` sends to replay over the live channel.
#[allow(clippy::large_enum_variant)]
pub enum LiveReplayMessage {
    /// One decoded, completed data set.
    Entries {
        slot: Slot,
        indices: Range<u32>,
        component: BlockComponent,
        slot_full: bool,
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
    component: BlockComponent,
    slot_full: bool,
}

/// Shared by reference across replay's parallel per-bank workers (hence
/// `DashMap`), owned by `ReplayStage`.
#[derive(Default)]
pub struct LiveReplayEntries {
    queued: DashMap<(Slot, u32), Queued>,
    /// Slots whose replay was restarted by a dump/purge while their shreds
    /// were left in the blockstore (see `replay_stage::purge_unconfirmed_slot`).
    /// Their data sets were already consumed from `queued` and will never be
    /// sent again, so they must be replayed from the persisted blockstore.
    /// A rare recovery path, not the steady-state one.
    replay_from_blockstore: DashSet<Slot>,
}

impl LiveReplayEntries {
    pub fn push(&self, message: LiveReplayMessage) {
        let (slot, indices, component, slot_full) = match message {
            LiveReplayMessage::Entries {
                slot,
                indices,
                component,
                slot_full,
            } => (slot, indices, component, slot_full),
            LiveReplayMessage::ReplayFromBlockstore { slot } => {
                self.remove_slot(slot);
                self.mark_replay_from_blockstore(slot);
                return;
            }
        };
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
        self.replay_from_blockstore.retain(|slot| *slot > root);
    }

    /// Drops everything queued for `slot`. Called when a slot is purged: its
    /// queued entries belong to a version of the block that is being
    /// discarded and must not mix with the repaired one.
    pub fn remove_slot(&self, slot: Slot) {
        self.queued.retain(|(queued_slot, _), _| *queued_slot != slot);
    }

    /// See `replay_from_blockstore`'s docs.
    pub fn mark_replay_from_blockstore(&self, slot: Slot) {
        self.replay_from_blockstore.insert(slot);
    }

    pub fn replays_from_blockstore(&self, slot: Slot) -> bool {
        self.replay_from_blockstore.contains(&slot)
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
        LiveReplayMessage::Entries {
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
    fn replay_from_blockstore_message_clears_queue_and_flags_slot() {
        let live = LiveReplayEntries::default();
        live.push(message(6, 4..7, 2, false));
        live.push(LiveReplayMessage::ReplayFromBlockstore { slot: 6 });
        assert!(live.replays_from_blockstore(6));
        assert!(live.drain_ready(6, 4).0.is_empty());
    }

    #[test]
    fn remove_slot_drops_only_that_slot() {
        let live = LiveReplayEntries::default();
        live.push(message(7, 0..2, 1, true));
        live.push(message(8, 0..2, 2, true));
        live.remove_slot(7);
        assert!(live.drain_ready(7, 0).0.is_empty());
        assert_eq!(live.drain_ready(8, 0).0, vec![entry_batch(2)]);
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
