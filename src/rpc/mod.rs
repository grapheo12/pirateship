use std::{ops::Deref, pin::Pin, sync::Arc};

pub mod server;
pub mod client;
pub mod auth;

/// Do not run these tests all together.
/// The tests needs a config directory.
/// Create it by running: sh scripts/gen_local_config.sh configs 7 scripts/local_template.json
#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub enum SenderType {
    Anon,
    Auth(String)
}

#[derive(Clone)]
pub struct Message(Arc<Vec<u8>>, usize, SenderType);

#[derive(Clone)]
pub struct MessageRef<'a>(&'a Vec<u8>, usize, &'a SenderType);

#[derive(Clone)]
pub struct PinnedMessage(Arc<Pin<Box<(Vec<u8>, usize, SenderType)>>>);

impl Message {
    
    pub fn from(arr: Vec<u8>, sz: usize, sender: SenderType) -> Message {
        Message(Arc::new(arr), sz, sender)
    }

    pub fn as_ref(&self) -> MessageRef {
        MessageRef(self.0.as_ref(), self.1, &self.2)
    }
}

impl<'a> MessageRef<'a> {
    pub fn from(arr: &'a Vec<u8>, sz: usize, sender: &'a SenderType) -> MessageRef<'a> {
        MessageRef(arr, sz, sender)
    }
}


impl Deref for Message {
    type Target = Vec<u8>;
    
    fn deref(&self) -> &Self::Target {
        &self.0
    }
    
}

impl<'a> MessageRef<'a> {
    pub fn size(&self) -> usize {
        self.1
    }

    pub fn sender(&self) -> &SenderType {
        &self.2
    }
}

impl<'a> Deref for MessageRef<'a> {
    type Target = Vec<u8>;
    
    fn deref(&self) -> &Self::Target {
        &self.0
    }
    
}

impl PinnedMessage {
    pub fn from(arr: Vec<u8>, sz: usize, sender: SenderType) -> PinnedMessage {
        PinnedMessage(Arc::new(Box::pin((arr, sz, sender))))
    }

    pub fn as_ref(&self) -> MessageRef {
        MessageRef(self.0.0.as_ref(), self.0.1, &self.0.2)
    }
}