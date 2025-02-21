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

mod serialize;
pub use serialize::*;

mod perf;
pub use perf::*;


pub mod channel {
    mod channel_tokio {
        pub type Sender<T> = tokio::sync::mpsc::Sender<T>;
        pub type Receiver<T> = tokio::sync::mpsc::Receiver<T>;
    
        pub fn make_channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>) {
            tokio::sync::mpsc::channel(buffer)
        }
    }

    mod channel_async {
        pub struct AsyncSenderWrapper<T>(async_channel::Sender<T>);

        impl<T> Clone for AsyncSenderWrapper<T> {
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }

        #[derive(Clone)]
        pub struct AsyncReceiverWrapper<T>(async_channel::Receiver<T>);

        impl<T> AsyncReceiverWrapper<T> {
            pub async fn recv(&self) -> Option<T> {
                match self.0.recv().await {
                    Ok(e) => Some(e),
                    Err(_) => None,
                }
            }
        }

        impl <T> AsyncSenderWrapper<T> {
            pub async fn send(&self, e: T) -> Result<(), async_channel::SendError<T>> {
                self.0.send(e).await
            }
        }


        pub type Sender<T> = AsyncSenderWrapper<T>;
        pub type Receiver<T> = AsyncReceiverWrapper<T>;
    
        pub fn make_channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>) {
            let (tx, rx) = async_channel::bounded(buffer);
            (AsyncSenderWrapper(tx), AsyncReceiverWrapper(rx))
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

    pub use channel_async::*;

}
