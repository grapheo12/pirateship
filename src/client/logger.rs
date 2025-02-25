use std::{collections::VecDeque, pin::Pin, sync::Arc, time::{Duration, Instant}};

use log::info;

use crate::utils::{channel::Receiver, timer::ResettableTimer};

pub enum ClientWorkerStat {
    CrashCommitLatency(Duration),
    ByzCommitLatency(Duration),
}

pub struct ClientStatLogger {
    stat_rx: Receiver<ClientWorkerStat>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,
    average_window: Duration,

    crash_commit_latency_window: VecDeque<(Instant /* when it was registered */, Duration /* latency value */)>,

    byz_commit_latency_window: VecDeque<(Instant, Duration)>,
}

impl ClientStatLogger {
    pub fn new(stat_rx: Receiver<ClientWorkerStat>, interval: Duration, average_window: Duration) -> Self {
        let log_timer = ResettableTimer::new(interval);
        Self {
            stat_rx,
            log_timer,
            average_window,
            crash_commit_latency_window: VecDeque::new(),
            byz_commit_latency_window: VecDeque::new(),
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
                while let Some((registered_time, _latency)) = self.crash_commit_latency_window.front() {
                    if registered_time.elapsed() > self.average_window {
                        self.crash_commit_latency_window.pop_front();
                    } else {
                        break;
                    }
                }
                self.crash_commit_latency_window.push_back((Instant::now(), latency));
            }
            ClientWorkerStat::ByzCommitLatency(latency) => {
                while let Some((registered_time, _latency)) = self.byz_commit_latency_window.front() {
                    if registered_time.elapsed() > self.average_window {
                        self.byz_commit_latency_window.pop_front();
                    } else {
                        break;
                    }
                }
                self.byz_commit_latency_window.push_back((Instant::now(), latency));
            }
        }
    }

    fn log_stats(&mut self) {
        let crash_commit_avg = if self.crash_commit_latency_window.len() > 0 {
            self.crash_commit_latency_window.iter()
                .fold(Duration::from_secs(0), |acc, (_, latency)| acc + *latency)
                .div_f64(self.crash_commit_latency_window.len() as f64)
        } else {
            Duration::from_secs(0)
        };
        let byz_commit_avg = if self.byz_commit_latency_window.len() > 0 {
            self.byz_commit_latency_window.iter()
                .fold(Duration::from_secs(0), |acc, (_, latency)| acc + *latency)
                .div_f64(self.byz_commit_latency_window.len() as f64)
        } else {
            Duration::from_secs(0)
        };

        info!("Average Crash commit latency: {:?}, Average Byz commit latency: {:?}", crash_commit_avg, byz_commit_avg);
    }
}


