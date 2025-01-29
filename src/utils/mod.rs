mod atomic_struct;
pub use atomic_struct::*;

mod agnostic_ref;
pub use agnostic_ref::*;

mod storage;
pub use storage::*;

mod file_storage;
pub use file_storage::*;

mod storage_service;
pub use storage_service::*;

pub mod workload_generators;


pub mod channel {
    mod channel_tokio {
        pub type Sender<T> = tokio::sync::mpsc::Sender<T>;
        pub type Receiver<T> = tokio::sync::mpsc::Receiver<T>;
    
        pub fn make_channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>) {
            tokio::sync::mpsc::channel(buffer)
        }
    }


    /// Kanal doesn't seem to have ordered delivery sometimes.
    mod channel_kanal {
        pub struct AsyncReceiverWrapper<T>(kanal::AsyncReceiver<T>);
        impl<T> AsyncReceiverWrapper<T> {
            pub async fn recv(&mut self) -> Option<T> {
                match self.0.recv().await {
                    Ok(e) => Some(e),
                    Err(_) => None,
                }
            }
        }
        pub type Sender<T> = kanal::AsyncSender<T>;
        pub type Receiver<T> = AsyncReceiverWrapper<T>;
    
        pub fn make_channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>) {
            let (tx, rx) = kanal::bounded_async(buffer);
            (tx, AsyncReceiverWrapper(rx))
        }
    }

    pub use channel_tokio::*;

}
