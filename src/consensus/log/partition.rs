use std::{cmp::Reverse, collections::BinaryHeap, io::{Error, ErrorKind}};

use tokio::sync::mpsc::Sender;

use crate::{crypto::hash, proto::consensus::ProtoBlock, utils::AgnosticRef};

use super::{entry::LogEntry, LogPartitionRequest};

#[derive(Clone, Debug)]
pub struct LogPartitionConfig {
    /// Block_n of the form (partition_id + k * partition_total + 1) 
    /// (1-based indexing)
    pub partition_id: u64,
    pub partition_total: u64,
}

pub(crate) struct OutOfOrderEntry(pub(crate) Reverse<u64>, LogEntry);

impl OutOfOrderEntry {
    pub(crate) fn new(block: ProtoBlock) -> Self {
        Self(Reverse(block.n), LogEntry::new(block))
    }
}

impl std::cmp::PartialEq for OutOfOrderEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl std::cmp::Eq for OutOfOrderEntry {

}

impl std::cmp::PartialOrd for OutOfOrderEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl std::cmp::Ord for OutOfOrderEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}


pub struct LogPartition {
    pub config: LogPartitionConfig,
    pub entries: Vec<LogEntry>,
    pub out_of_order_entries: BinaryHeap<OutOfOrderEntry>,
}

impl LogPartition {
    pub fn new(config: LogPartitionConfig) -> Self {
        LogPartition {
            config,
            entries: vec![],
            out_of_order_entries: BinaryHeap::new(),
        }
    }

    fn would_be_out_of_order(&self, n: u64) -> bool {
        let last_n = self.last();
        if last_n == 0 {
            // No entries, so the first n, should be partition_id + 1
            n != (self.config.partition_id + 1)
        } else {
            // Next index mys
            n != (last_n + self.config.partition_total)
        }
        
    }

    pub fn push_unchecked(&mut self, block: ProtoBlock) -> Result<u64, Error> {
        if block.n % self.config.partition_total != self.config.partition_id + 1 {
            return Err(Error::new(ErrorKind::InvalidData, "Wrong partition"))
        }

        if self.entries.len() > 0 {
            let last_n = self.entries.last().unwrap().block.n;
            if block.n <= last_n {
                return Err(Error::new(ErrorKind::InvalidData, "block.n <= last_n"))
            }

            if self.would_be_out_of_order(block.n) {
                let oo_entry = OutOfOrderEntry::new(block);
                self.out_of_order_entries.push(oo_entry);
                return Err(Error::new(ErrorKind::Other, "Out of order"))
            }
        }



        let mut block_n = block.n;
        self.entries.push(LogEntry::new(block));

        while self.out_of_order_entries.len() > 0 {
            let oo_entry = self.out_of_order_entries.peek().unwrap();
            if oo_entry.0.0 > block_n + self.config.partition_total {
                break;
            }

            self.entries.push(oo_entry.1.clone());
            block_n = oo_entry.0.0;
            self.out_of_order_entries.pop();

            // Clear out all the repeated entries.
            while self.out_of_order_entries.len() > 0 {
                if self.out_of_order_entries.peek().unwrap().0.0 == block_n {
                    self.out_of_order_entries.pop();
                } else {
                    break;
                }
            }

        }

        Ok(block_n)
    }

    pub async fn push(&mut self, mut block: ProtoBlock, req_chan: &Vec<Sender<LogPartitionRequest>>) -> Result<u64, Error> {
        if self.would_be_out_of_order(block.n) {
            return self.push_unchecked(block)
        }


    }

    pub fn last_n(&self) -> Option<u64> {
        if self.entries.len() == 0 {
            return None
        }

        Some(self.entries.last().unwrap().block.n)
    }

    pub fn last(&self) -> u64 {
        self.last_n().unwrap_or(0)
    }

    pub fn get(&self, n: u64) -> Result<AgnosticRef<LogEntry>, Error> {
        if n > self.last() || n == 0 {
            return Err(Error::new(ErrorKind::InvalidInput, format!("Out of bounds {}, last() = {}", n, self.last())));
        }

        // #[cfg(feature = "storage")]
        // if n <= self.gc_hiwm {
        //     let res = self.get_gc_block(n);
        //     match res {
        //         Ok(entry) => {
        //             return Ok(AgnosticRef::from(entry))
        //         },
        //         Err(e) => {
        //             return Err(e)
        //         },
        //     }
        // }

        // #[cfg(feature = "storage")]
        // return Ok(AgnosticRef::from(
        //     self.entries.get((n - self.gc_hiwm - 1) as usize).unwrap()
        // ));


        // #[cfg(not(feature = "storage"))]
        Ok(AgnosticRef::from(
            self.entries.get((n - 1) as usize).unwrap()
        ))
    }

    pub fn hash_at_n(&self, n: u64) -> Option<Vec<u8>> {
        if n > self.last() {
            return None;
        }

        if n == 0 {
            let buf = Vec::new();
            Some(hash(&buf))
        } else {
            Some(self.get(n).unwrap().block_hash.clone())
        }
    }
}