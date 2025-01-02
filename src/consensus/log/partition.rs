use std::{cmp::Reverse, collections::BinaryHeap, io::{Error, ErrorKind}};

use crate::proto::consensus::ProtoBlock;

use super::entry::LogEntry;

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

    pub fn push(&mut self, block: ProtoBlock) -> Result<u64, Error> {
        if block.n % self.config.partition_total != self.config.partition_id + 1 {
            return Err(Error::new(ErrorKind::InvalidData, "Wrong partition"))
        }

        if self.entries.len() > 0 {
            let last_n = self.entries.last().unwrap().block.n;
            if block.n <= last_n {
                return Err(Error::new(ErrorKind::InvalidData, "block.n <= last_n"))
            }

            if block.n != last_n + self.config.partition_total {
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

    pub fn last_n(&self) -> Option<u64> {
        if self.entries.len() == 0 {
            return None
        }

        Some(self.entries.last().unwrap().block.n)
    }
}