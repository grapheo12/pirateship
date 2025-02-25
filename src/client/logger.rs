use std::{pin::Pin, sync::Arc, time::Duration};

use log::info;

use crate::utils::{channel::Receiver, timer::ResettableTimer};

pub enum ClientWorkerStat {
    CrashCommitLatency(Duration),
    ByzCommitLatency(Duration),
}

struct ClientStatLogger {
    stat_rx: Receiver<ClientWorkerStat>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    crash_commit_latency_sum: Duration,
    crash_commit_latency_count: usize,

    byz_commit_latency_sum: Duration,
    byz_commit_latency_count: usize,
}

impl ClientStatLogger {
    pub fn new(stat_rx: Receiver<ClientWorkerStat>, interval: Duration) -> Self {
        let log_timer = ResettableTimer::new(interval);
        Self {
            stat_rx,
            log_timer,
            crash_commit_latency_sum: Duration::from_secs(0),
            crash_commit_latency_count: 0,
            byz_commit_latency_sum: Duration::from_secs(0),
            byz_commit_latency_count: 0,
        }
    }

    pub async fn run(&mut self) {
        self.log_timer.run().await;

        loop {

            tokio::select! {
                _ = self.log_timer.wait() => {
                    self.log_stats();
                }
                entry = self.stat_rx.recv() => {
                    if let Some(stat) = entry {
                        self.collect_stat(stat);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn collect_stat(&mut self, stat: ClientWorkerStat) {
        match stat {
            ClientWorkerStat::CrashCommitLatency(latency) => {
                self.crash_commit_latency_sum += latency;
                self.crash_commit_latency_count += 1;
            }
            ClientWorkerStat::ByzCommitLatency(latency) => {
                self.byz_commit_latency_sum += latency;
                self.byz_commit_latency_count += 1;
            }
        }
    }

    fn log_stats(&mut self) {
        let crash_commit_avg = if self.crash_commit_latency_count > 0 {
            self.crash_commit_latency_sum.div_f64(self.crash_commit_latency_count as f64)
        } else {
            Duration::from_secs(0)
        };
        let byz_commit_avg = if self.byz_commit_latency_count > 0 {
            self.byz_commit_latency_sum.div_f64(self.byz_commit_latency_count as f64)
        } else {
            Duration::from_secs(0)
        };

        info!("Average Crash commit latency: {:?}, Average Byz commit latency: {:?}", crash_commit_avg, byz_commit_avg);
    }
}


