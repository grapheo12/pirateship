
#[cfg(feature="dummy_2pc")]
mod dummy_2pc;
pub use dummy_2pc::algorithm;

#[cfg(feature="raft")]
mod raft;
#[cfg(feature="raft")]
use raft::*;

#[cfg(feature="jolteon")]
mod jolteon;
#[cfg(feature="jolteon")]
use jolteon::*;

#[cfg(feature="pirateship")]
mod pirateship;
#[cfg(feature="pirateship")]
use pirateship::*;
