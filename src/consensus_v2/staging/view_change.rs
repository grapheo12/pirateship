use std::collections::HashMap;

use crate::{consensus_v2::{block_broadcaster::BlockBroadcasterCommand, block_sequencer::BlockSequencerControlCommand, fork_receiver::ForkReceiverCommand, pacemaker::PacemakerCommand}, proto::consensus::ProtoViewChange, rpc::SenderType};

use super::Staging;


impl Staging {
    pub(super) async fn process_view_change_message(&mut self, cmd: PacemakerCommand) -> Result<(), ()> {
        match cmd {
            PacemakerCommand::UpdateView(view_num, config_num) => {
                self.update_view(view_num, config_num).await;
            },
            PacemakerCommand::NewViewForks(view_num, config_num, forks) => {
                self.propose_new_view(view_num, config_num, forks).await;
            },
            _ => {
                unreachable!("Invalid message from pacemaker");
            }
        }
        Ok(())
    }
    
    pub(super) async fn update_view(&mut self, view_num: u64, config_num: u64) {
        if self.view >= view_num || self.config_num >= config_num {
            return;
        }

        self.view = view_num;
        self.config_num = config_num;
        self.view_is_stable = false;


        // Send the signal to everywhere
        self.block_sequencer_command_tx.send(BlockSequencerControlCommand::NewUnstableView(self.view, self.config_num)).await.unwrap();
        self.fork_receiver_command_tx.send(ForkReceiverCommand::UpdateView(self.view, self.config_num)).await.unwrap();
        self.pacemaker_tx.send(PacemakerCommand::UpdateView(self.view, self.config_num)).await.unwrap();
    }

    async fn propose_new_view(&mut self, view_num: u64, config_num: u64, forks: HashMap<SenderType, ProtoViewChange>) {
        // TODO
    }

}