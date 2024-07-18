use std::{pin::Pin, sync::atomic::AtomicBool, time::Duration};

use tokio::{sync::mpsc, time::sleep};

pub struct ResettableTimer {
    pub timeout: Duration,
    tx: mpsc::Sender<bool>,
    rx: mpsc::Receiver<bool>,
    is_cancelled: AtomicBool
}

impl ResettableTimer {
    pub fn new(timeout: Duration) -> Pin<Box<Self>> {
        let (tx, rx) = mpsc::channel(1);
        Box::pin(ResettableTimer {
            timeout, tx, rx,
            is_cancelled: AtomicBool::new(false)
        })
    }

    pub async fn run(self: Pin<Box<Self>>) {
        let tx = self.tx.clone();
        let tout = self.timeout;
        tokio::spawn(async move {
            loop {
                // This sleep has ms accuracy.
                sleep(tout);

                // Logic: Send the timeout signal, only if the is_cancelled is false.
                // If is_cancelled, reset it to false and skip sending this signal.
                // Sleeps a little extra: Cannot guarantee very accurate resets.
                // |-----(Timeout 1)-----|-----(Timeout 2)-----|
                //       ^                     ^               ^
                //       |                     |               |
                //       Reset                 |               |
                //                      Should have fired here |
                //                                    Fires here

                match self.is_cancelled.compare_exchange(
                    true,     // If it is currenty true
                    false,        // Set it to false
                    std::sync::atomic::Ordering::Release,    
                    std::sync::atomic::Ordering::Relaxed)
                {
                    Ok(_) => {
                        // Setting was successful.
                        // Do nothing. Skip this tick.
                    },
                    Err(_) => {
                        let _ = tx.send(true).await;
                    }
                };
                
            }
        });
    }

    pub async fn wait(self: &mut Pin<Box<Self>>) -> bool {
        self.rx.recv().await;
        true
    }

    pub fn reset(self: &Pin<Box<Self>>) {
        let _ = self.is_cancelled.compare_exchange(
            false,     // If it is currenty false
            true,        // Set it to true
            std::sync::atomic::Ordering::Acquire,    
            std::sync::atomic::Ordering::Relaxed);
    }
}