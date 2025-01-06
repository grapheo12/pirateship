// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::{ops::Index, sync::atomic::Ordering};

use log::{debug, error, info, warn};
use prost::Message;
use tokio::sync::{mpsc::UnboundedSender, MutexGuard};

use crate::{
    consensus::{
        handler::PinnedServerContext,
        log::Log,
    }, crypto::cmp_hash, proto::{
        checkpoint::{ProtoBackFillRequest, ProtoBackFillResponse}, consensus::{
            ProtoFork,
            ProtoViewChange,
        }, rpc::ProtoPayload
    }, rpc::{
        client::PinnedClient,
        server::LatencyProfile,
        PinnedMessage,
    }
};

/// Leader of new view receives View Change messages from (n - u) followers.
/// The fork in these View Change messages maybe divergent from the leaders.
/// This function backfills from the followers until the fork included in the View Change has a common prefix with the leader.
/// For example:
/// Leader's fork: 1 <- 2 <- 3 <- 4 <- 5 <- 6 <- 7 <- 8 <- 9 <- 10
///                                \
/// VC fork:                        \          <- 7' <- 8' <- 9'
///                                  \
/// After backfill, VC fork:          \- 5' <- 6' <- 7' <- 8' <- 9'
/// Returns the modified View Change message.
pub async fn maybe_backfill_fork_till_prefix_match(ctx: PinnedServerContext, client: PinnedClient, vc: &ProtoViewChange, sender: &String) -> Option<ProtoViewChange> {
    // If vc_fork extends my fork, then the highest matching prefix will be my fork.last() or something extending from it.
    // Backfill seq nums between fork.last() and start of vc_fork.
    let mut vc_fork = vc.fork.clone().unwrap();
    {
        let fork = ctx.state.fork.lock().await;
        vc_fork = maybe_backfill_fork_till_last_match(&ctx, &client, &vc_fork, fork.last(), sender).await;
    }

    // Guarantees that vc_fork.blocks[0].n <= fork.last().
    
    while vc_fork.blocks[0].n > 0 {
        // Check if vc_fork's parent is in my fork.
        let my_parent_hash = {
            let fork = ctx.state.fork.lock().await;
            match fork.hash_at_n(vc_fork.blocks[0].n - 1) {
                Some(h) => h,
                None => {
                    return None; // This is clearly an error!
                }
            }
        };
    
        if cmp_hash(&vc_fork.blocks[0].parent, &my_parent_hash) {
            break;
        }

        // If not, backfill 1000 more blocks and do this again.
        let start = if vc_fork.blocks[0].n >= 1000 {
            vc_fork.blocks[0].n - 1000
        }else {
            0
        };
        
        vc_fork = maybe_backfill_fork(&ctx, &client, &vc_fork, sender,
            start,   // Fetch 1000 blocks at a time. A better approach is to bin search first.
            vc_fork.blocks[0].n - 1).await;
    }

    Some(ProtoViewChange {
        fork: Some(vc_fork),
        view: vc.view,
        fork_sig: vc.fork_sig.clone(),
        fork_len: vc.fork_len,
        fork_last_qc: vc.fork_last_qc.clone(),
        config_num: vc.config_num,
    })
}


/// Server side of backfilling. Clients will be blocking on this to receive backfill responses.
pub async fn do_process_backfill_request(ctx: PinnedServerContext, ack_tx: &mut oneshot::Sender<(PinnedMessage, LatencyProfile)>, bfr: &ProtoBackFillRequest, _sender: &String) {
    let block_start = bfr.block_start;
    let block_end = bfr.block_end;

    let mut profile = LatencyProfile::new();
    let fork = ctx.state.fork.try_lock();
    let fork = match fork {
        Ok(f) => {
            debug!("do_process_backfill_request: Fork locked");
            f
        },
        Err(e) => {
            info!("do_process_backfill_request: Fork is locked, waiting for it to be unlocked: {}", e);
            let fork = ctx.state.fork.lock().await;
            info!("do_process_backfill_request: Fork locked");
            fork  
        }
    };

    // Fetch all blocks from block_start to block_end.
    let mut resp_fork = fork.serialize_range(block_start, block_end);

    #[cfg(feature = "evil")]
    if ctx.simulate_byz_behavior && ctx.view_is_stable.load(Ordering::SeqCst) {
        let send_list = ctx.send_list.get();
        let mid = send_list.len() / 2;
        if send_list.iter().position(|e| e.eq(_sender)).unwrap_or(send_list.len()) >= mid {
            // You must get the equivocated blocks.
            let eq_block_store = ctx.state.equivocated_blocks.lock().await;
            for block in resp_fork.blocks.iter_mut() {
                if block.n < 5000 {
                    continue;
                }

                let _blk = eq_block_store.get(&block.n);
                match _blk {
                    Some(_b) => {
                        *block = _b.clone();
                    },
                    None => {}
                }
            }
        }
    }
    profile.register("Backfill done");

    let response = ProtoBackFillResponse {
        fork: Some(resp_fork),
    };
    let rpc_msg_body = ProtoPayload {
        message: Some(crate::proto::rpc::proto_payload::Message::BackfillResponse(response)),
    };
    let mut buf = Vec::new();
    rpc_msg_body.encode(&mut buf).unwrap();
    let sz = buf.len();
    let reply = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);
    ack_tx.send((reply, profile)).unwrap();

}

/// Fills up the gap between fork_last and the first block in the given fork.
/// If fork_last is higher, no change.
pub async fn maybe_backfill_fork_till_last_match<'a>(ctx: &PinnedServerContext, client: &PinnedClient, f: &ProtoFork, fork_last: u64, sender: &String) -> ProtoFork {
    let start = fork_last + 1;
    let end = f.blocks[0].n - 1;
    if start > end {
        return f.clone();
    }

    maybe_backfill_fork(ctx, client, f, sender, start, end).await

    
}

/// Fills blocks from block_start to block_end and appends them to f.
pub async fn maybe_backfill_fork<'a>(_ctx: &PinnedServerContext, client: &PinnedClient, f: &ProtoFork, sender: &String, block_start: u64, block_end: u64) -> ProtoFork {
    if f.blocks.len() == 0 {
        return f.clone();
    }

    
    let backfill_req = ProtoBackFillRequest {
        block_start,
        block_end
    };
    info!("Backfilling fork from {} {:?}", sender, backfill_req);

    let mut buf = Vec::new();
    let rpc_msg_body = ProtoPayload {
        message: Some(crate::proto::rpc::proto_payload::Message::BackfillRequest(backfill_req)),
    };
    let backfill_resp = if let Ok(_) = rpc_msg_body.encode(&mut buf) {
        let sz = buf.len();
        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

        // Current task blocks until a reply is received.
        // WARNING: This is vulnerable to a DoS attack.
        let resp = PinnedClient::send_and_await_reply(client, sender, request.as_ref()).await;
        if let Err(e) = resp {
            warn!("Error backfilling: {}", e);
            return f.clone();
        }

        let resp = resp.unwrap();
        let resp = resp.as_ref();
        let body = match ProtoPayload::decode(&resp.0.as_slice()[0..resp.1]) {
            Ok(b) => b,
            Err(e) => {
                warn!("Parsing problem: {}", e.to_string());
                debug!("Original message: {:?} {:?}", &resp.0, &resp.1);
                return f.clone();
            }
        };

        let backfill_resp = if let Some(crate::proto::rpc::proto_payload::Message::BackfillResponse(r)) = body.message {
            r
        }else{
            warn!("Invalid backfill response");
            return f.clone();
        };

        backfill_resp
    } else {
        error!("Error encoding backfill request");
        return f.clone();
    };

    // No sanity checking here. The log push/overwrite will fail if the fork is bad.

    if let None = backfill_resp.fork {
        return f.clone();
    }
    
    let mut res_fork = backfill_resp.fork.unwrap();
    res_fork.blocks.extend(f.blocks.clone());
    info!("Backfilled fork range from {} to {}", f.blocks[0].n, f.blocks[f.blocks.len()-1].n);

    res_fork

}
