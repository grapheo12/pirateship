/// This is defunct



use std::{collections::VecDeque, ops::{Deref, DerefMut}, pin::Pin, sync::Mutex};


/// RPCs in a Raft-like consensus system has this property:
/// If seq num `n` is done, all previous RPCs are also done.
/// This is because a separate system is going to backfill the hashchain.
/// Votes are only going to come for filled up chain.
/// For diverse RPCs, if seq num `n` is done, this is still allow `n - k` to pass.
/// @todo: Handle Sequence number overflow. This is not a problem now.
/// But will be in production.
/// A potential fix is to reset it after every view change.
#[derive(Clone, Debug)]
pub struct SequencerNumberGenerator {
    pub k: usize,
    pub next_seq_num: u64,
    pub max_completed_quorum: u64,
    pub active_diverse_quorums: VecDeque<u64>,
    pub lagging_diverse_quorums: VecDeque<u64>
}

pub struct PinnedSequenceNumberGenerator {
    gen: Pin<Box<Mutex<SequencerNumberGenerator>>>
}

impl PinnedSequenceNumberGenerator {
    fn new(k: u64) -> PinnedSequenceNumberGenerator {
        PinnedSequenceNumberGenerator {
            gen: Box::pin(Mutex::new(SequencerNumberGenerator {
                k: k as usize,
                next_seq_num: 1,
                max_completed_quorum: 0,
                active_diverse_quorums: VecDeque::new(),
                lagging_diverse_quorums: VecDeque::new()
            }))
        }
    }
}

impl Deref for PinnedSequenceNumberGenerator {
    type Target = Pin<Box<Mutex<SequencerNumberGenerator>>>;

    fn deref(&self) -> &Self::Target {
        &self.gen
    }
}

impl DerefMut for PinnedSequenceNumberGenerator {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.gen
    }
}

impl PinnedSequenceNumberGenerator {
    fn register_fast(&mut self) -> u64 {
        let mut _self = self.lock().unwrap();
        let res = _self.next_seq_num;
        _self.next_seq_num += 1;

        res
    }

    fn register_diverse(&mut self) -> u64 {
        let mut _self = self.lock().unwrap();
        let res = _self.next_seq_num;
        _self.next_seq_num += 1;

        // Invariant: _self.active_diverse_quorum is sorted.
        _self.active_diverse_quorums.push_back(res);

        res
    }

    fn close_upto(&mut self, n: u64) {
        let mut _self = self.lock().unwrap();
        if n <= _self.max_completed_quorum {
            return;
        }

        _self.max_completed_quorum = n;
    }

    fn close_diverse_upto(&mut self, n: u64) {
        self.close_upto(n);
        let mut _self = self.lock().unwrap();

        loop {
            match _self.active_diverse_quorums.pop_front() {
                Some(x) => {
                    if x <= n {
                        // This quorum is done.
                        // But may still receive votes later.
                        // Invariant: _self.lagging_diverse_quorum is sorted.

                        _self.lagging_diverse_quorums.push_back(x);

                    }else{
                        // Oops, overshoot!
                        // Invariant: _self.active_diverse_quorum is sorted.
                        _self.active_diverse_quorums.push_front(x);
                        break;
                    }
                },
                None => {
                    break;
                }
            }
        }

        // Now trim the lagging diverse quorums so only k of them are left.
        while _self.lagging_diverse_quorums.len() > _self.k {
            // Invariant: _self.lagging_diverse_quorum is sorted.
            _self.lagging_diverse_quorums.pop_front();
        }
    }

    fn is_allowed(&self, n: u64) -> bool {
        let _self = self.lock().unwrap();

        // Fast quorum rule
        if n > _self.max_completed_quorum {
            return true;
        }


        // Is it an active diverse quorum?
        // n < _self.max_completed_quorum but also is in active_diverse_quorum
        // Can only happen if you called close_upto() instead of close_diverse_upto()
        // Why? 
        // block 0 | Signed(block 1) | block 2 
        //                              ^
        //                              |___ Voted for block 2 directly after block 0
        //                                   AND did not send signature for block 1.
        // In this case a signed vote for a later block will close this active diverse quorum.

        if let Ok(_) = _self.active_diverse_quorums.binary_search(&n) {
            return true;
        }

        // Is it in lagging diverse quorum?
        // I don't need to worry about the size here.
        if let Ok(_) = _self.lagging_diverse_quorums.binary_search(&n) {
            return true;
        }

        false

    }
}