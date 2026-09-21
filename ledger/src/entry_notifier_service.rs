use {
    crate::entry_notifier_interface::{EntryNotifierArc, EntryUpdateParentInfo},
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded},
    solana_clock::{BankId, Slot},
    solana_entry::{block_component::VersionedBlockFooter, entry::EntrySummary},
    solana_measure::measure::Measure,
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

const METRICS_REPORTING_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Default)]
struct EntryNotifierServiceMetrics {
    notify_entry_count: u64,
    notify_entry_elapsed_us: u64,
    notify_block_footer_count: u64,
    notify_block_footer_elapsed_us: u64,
    notify_update_parent_count: u64,
    notify_update_parent_elapsed_us: u64,
    max_receiver_len: usize,
}

impl EntryNotifierServiceMetrics {
    const NAME: &str = "entry-notifier-service-timing";

    fn report(&self) {
        datapoint_info!(
            Self::NAME,
            ("notify_entry_count", self.notify_entry_count as i64, i64),
            (
                "notify_entry_elapsed_us",
                self.notify_entry_elapsed_us as i64,
                i64
            ),
            (
                "notify_block_footer_count",
                self.notify_block_footer_count as i64,
                i64
            ),
            (
                "notify_block_footer_elapsed_us",
                self.notify_block_footer_elapsed_us as i64,
                i64
            ),
            (
                "notify_update_parent_count",
                self.notify_update_parent_count as i64,
                i64
            ),
            (
                "notify_update_parent_elapsed_us",
                self.notify_update_parent_elapsed_us as i64,
                i64
            ),
            ("max_receiver_len", self.max_receiver_len as i64, i64),
        );
    }
}

pub enum EntryNotification {
    Entry {
        slot: Slot,
        bank_id: BankId,
        index: usize,
        entry: EntrySummary,
        starting_transaction_index: usize,
    },
    BlockFooter {
        slot: Slot,
        bank_id: BankId,
        block_footer: Box<VersionedBlockFooter>,
    },
    UpdateParent(EntryUpdateParentInfo),
}

pub type EntryNotifierSender = Sender<EntryNotification>;
pub type EntryNotifierReceiver = Receiver<EntryNotification>;

pub struct EntryNotifierService {
    sender: EntryNotifierSender,
    thread_hdl: JoinHandle<()>,
}

impl EntryNotifierService {
    pub fn new(entry_notifier: EntryNotifierArc, exit: Arc<AtomicBool>) -> Self {
        let (entry_notification_sender, entry_notification_receiver) = unbounded();
        let thread_hdl = Builder::new()
            .name("solEntryNotif".to_string())
            .spawn(move || {
                let mut metrics = EntryNotifierServiceMetrics::default();
                let mut last_report = Instant::now();
                loop {
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }

                    if let Err(RecvTimeoutError::Disconnected) = Self::notify(
                        &entry_notification_receiver,
                        entry_notifier.clone(),
                        &mut metrics,
                    ) {
                        break;
                    }

                    if last_report.elapsed() > METRICS_REPORTING_INTERVAL {
                        metrics.report();
                        metrics = EntryNotifierServiceMetrics::default();
                        last_report = Instant::now();
                    }
                }
            })
            .unwrap();
        Self {
            sender: entry_notification_sender,
            thread_hdl,
        }
    }

    fn notify(
        entry_notification_receiver: &EntryNotifierReceiver,
        entry_notifier: EntryNotifierArc,
        metrics: &mut EntryNotifierServiceMetrics,
    ) -> Result<(), RecvTimeoutError> {
        let notification = entry_notification_receiver.recv_timeout(Duration::from_secs(1))?;
        metrics.max_receiver_len = metrics
            .max_receiver_len
            .max(entry_notification_receiver.len());
        match notification {
            EntryNotification::Entry {
                slot,
                bank_id,
                index,
                entry,
                starting_transaction_index,
            } => {
                let mut notify_entry_elapsed = Measure::start("notify_entry_elapsed");
                entry_notifier.notify_entry(
                    slot,
                    bank_id,
                    index,
                    &entry,
                    starting_transaction_index,
                );
                notify_entry_elapsed.stop();
                metrics.notify_entry_count += 1;
                metrics.notify_entry_elapsed_us += notify_entry_elapsed.as_us();
            }
            EntryNotification::BlockFooter {
                slot,
                bank_id,
                block_footer,
            } => {
                let mut notify_block_footer_elapsed = Measure::start("notify_block_footer_elapsed");
                entry_notifier.notify_block_footer(slot, bank_id, block_footer.as_ref());
                notify_block_footer_elapsed.stop();
                metrics.notify_block_footer_count += 1;
                metrics.notify_block_footer_elapsed_us += notify_block_footer_elapsed.as_us();
            }
            EntryNotification::UpdateParent(update_parent) => {
                let mut notify_update_parent_elapsed =
                    Measure::start("notify_update_parent_elapsed");
                entry_notifier.notify_entry_update_parent(&update_parent);
                notify_update_parent_elapsed.stop();
                metrics.notify_update_parent_count += 1;
                metrics.notify_update_parent_elapsed_us += notify_update_parent_elapsed.as_us();
            }
        }
        Ok(())
    }

    pub fn sender(&self) -> &EntryNotifierSender {
        &self.sender
    }

    pub fn sender_cloned(&self) -> EntryNotifierSender {
        self.sender.clone()
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::entry_notifier_interface::EntryNotifier,
        solana_entry::block_component::BlockFooterV1, solana_hash::Hash, std::sync::Mutex,
    };

    #[derive(Debug, PartialEq, Eq)]
    enum TestEvent {
        Entry {
            slot: Slot,
            bank_id: BankId,
            index: usize,
            starting_transaction_index: usize,
        },
        BlockFooter {
            slot: Slot,
            bank_id: BankId,
            block_footer: Box<VersionedBlockFooter>,
        },
        UpdateParent(Slot, BankId, Slot, Hash),
    }

    #[derive(Default)]
    struct TestEntryNotifier {
        events: Mutex<Vec<TestEvent>>,
    }

    impl EntryNotifier for TestEntryNotifier {
        fn notify_entry(
            &self,
            slot: Slot,
            bank_id: BankId,
            index: usize,
            _entry: &EntrySummary,
            starting_transaction_index: usize,
        ) {
            self.events.lock().unwrap().push(TestEvent::Entry {
                slot,
                bank_id,
                index,
                starting_transaction_index,
            });
        }

        fn notify_block_footer(
            &self,
            slot: Slot,
            bank_id: BankId,
            block_footer: &VersionedBlockFooter,
        ) {
            self.events.lock().unwrap().push(TestEvent::BlockFooter {
                slot,
                bank_id,
                block_footer: Box::new(block_footer.clone()),
            });
        }

        fn notify_entry_update_parent(&self, update_parent: &EntryUpdateParentInfo) {
            self.events.lock().unwrap().push(TestEvent::UpdateParent(
                update_parent.slot,
                update_parent.cleared_bank_id,
                update_parent.parent_slot,
                update_parent.parent_block_id,
            ));
        }
    }

    #[test]
    fn test_forwards_entry_notifications_in_order() {
        let (sender, receiver) = unbounded();
        let notifier = Arc::new(TestEntryNotifier::default());
        let block_footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: 123,
            block_user_agent: b"test-validator".to_vec(),
            block_final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        });
        let parent_block_id = Hash::new_unique();

        sender
            .send(EntryNotification::Entry {
                slot: 42,
                bank_id: 9,
                index: 3,
                entry: EntrySummary {
                    num_hashes: 1,
                    hash: Hash::new_unique(),
                    num_transactions: 2,
                },
                starting_transaction_index: 7,
            })
            .unwrap();
        sender
            .send(EntryNotification::UpdateParent(EntryUpdateParentInfo {
                slot: 42,
                cleared_bank_id: 9,
                parent_slot: 40,
                parent_block_id,
            }))
            .unwrap();
        sender
            .send(EntryNotification::BlockFooter {
                slot: 42,
                bank_id: 9,
                block_footer: Box::new(block_footer.clone()),
            })
            .unwrap();

        let mut metrics = EntryNotifierServiceMetrics::default();
        EntryNotifierService::notify(&receiver, notifier.clone(), &mut metrics).unwrap();
        EntryNotifierService::notify(&receiver, notifier.clone(), &mut metrics).unwrap();
        EntryNotifierService::notify(&receiver, notifier.clone(), &mut metrics).unwrap();

        assert_eq!(
            *notifier.events.lock().unwrap(),
            vec![
                TestEvent::Entry {
                    slot: 42,
                    bank_id: 9,
                    index: 3,
                    starting_transaction_index: 7,
                },
                TestEvent::UpdateParent(42, 9, 40, parent_block_id),
                TestEvent::BlockFooter {
                    slot: 42,
                    bank_id: 9,
                    block_footer: Box::new(block_footer),
                },
            ]
        );
    }
}
