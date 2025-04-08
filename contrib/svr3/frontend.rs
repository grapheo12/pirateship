use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use log::{debug, warn};
use pft::consensus_v2::batch_proposal::TxWithAckChanTag;
use prost::Message;
use serde_json::value;
use tokio::sync::{mpsc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use pft::config::Config;
use pft::crypto::{KeyStore, hash};
use pft::proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase};
use pft::rpc::client::Client;
use pft::rpc::{PinnedMessage, SenderType};
use pft::{proto::{client::{self, ProtoClientReply, ProtoClientRequest}, rpc::ProtoPayload}, rpc::client::PinnedClient, utils::channel::{make_channel, Receiver, Sender}};
use crate::payloads::{RecoverSecretPayload, RegisterPayload, StoreSecretPayload};



struct AppState {
    /// Global channel to feed into the consensusNode.
    batch_proposer_tx: Sender<TxWithAckChanTag>,
    /// Separate client instance for byz commit probe, so it doesn't interfere with other requests
    /// on the same worker thread.
    /// Arced since it is shared across threads.
    probe_for_byz_commit: Arc<AtomicBool>,
    /// Only a per-thread client tag counter remains.
    curr_client_tag: AtomicU64,
}

#[post("/auth")]
async fn auth(payload: web::Json<RegisterPayload>, data: web::Data<AppState>) -> impl Responder {
    let username = payload.username.clone();
    let password = payload.password.clone();
    let client_tag = &data.curr_client_tag;

    // Query KMS for username.
    let transaction_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec![username.clone().into_bytes()],
    };

    let result = match send(vec![transaction_op], true, &data).await {
        Ok(response) => response,
        Err(e) => return e,
    };

    if !result.is_empty() {
        return HttpResponse::Conflict().json(serde_json::json!({
            "message": "username already exists",
        }));
    }

    // Username does not exist; create new username and password.
    let create_user_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Write.into(),
        operands: vec![username.clone().into_bytes(), hash(&password.into_bytes())],
    };

    let _ = match send(vec![create_user_op], false, &data).await {
        Ok(response) => response,
        Err(e) => return e,
    };

    // Get list of users then add user to list of users.
    let get_user_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec!["user".as_bytes().to_vec()],
    };

    let get_user_result = match send(vec![get_user_op], true, &data).await {
        Ok(response) => response,
        Err(e) => return e,
    };

    let mut users: Vec<String> = Vec::new();
    if !get_user_result.is_empty() {
        users = serde_json::from_slice(&get_user_result[0]).expect("Deserialization failed");
    }

    users.push(username.clone());
    let serialized_users: Vec<u8> = serde_json::to_vec(&users).expect("Serialization failed");

    let update_users_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Write.into(),
        operands: vec!["user".as_bytes().to_vec(), serialized_users],
    };

    let _ = match send(vec![update_users_op],false, &data).await {
        Ok(response) => response,
        Err(e) => return e,
    };

    HttpResponse::Ok().json(serde_json::json!({
        "message": "user created",
        "users": users,
    }))
}

#[post("/storesecret")]
async fn storeSecret(payload: web::Json<StoreSecretPayload>, data: web::Data<AppState>) -> impl Responder {
    let username = payload.username.clone();
    let password = payload.password.clone();
    let val = payload.val.clone();
    let pin = payload.pin.clone();

    let client_tag = &data.curr_client_tag;

    match authenticate_user(username, password,&data).await {
        Ok(valid) => valid,
        Err(e) => return e,
    };

    //register pin ,store secret
    let mut user_pin = "pin:".to_string();
    let mut user_secret = "secret:".to_string();

    user_pin.push_str(&payload.username);
    user_secret.push_str(&payload.username);

    let write_user_pin_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Write.into(),
        operands: vec![user_pin.into_bytes(), pin.into_bytes()],
    };

    let result = match send(vec![write_user_pin_op], false, &data).await {
        Ok(response) => response,
        Err(e) => return e,
    };

    let write_user_secret_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Write.into(),
        operands: vec![user_secret.into_bytes(), val.into_bytes()],
    };

    let result = match send(vec![write_user_secret_op], true, &data).await {
        Ok(response) => response,
        Err(e) => return e,
    };

    HttpResponse::Ok().json(serde_json::json!({
        "message": "user secret store",
    }))
}

#[get("/recoversecret")]
async fn recoverSecret(payload: web::Json<RecoverSecretPayload>, data: web::Data<AppState>) -> impl Responder {
    let username = payload.username.clone();
    let password = payload.password.clone();
    let pin = payload.pin.clone();
    let client_tag = &data.curr_client_tag;

    match authenticate_user(username, password, &data).await {
        Ok(valid) => valid,
        Err(e) => return e,
    };

    //compare pin if correct --> return secret; else --> increment pin count
    let mut user_pin = "pin:".to_string();
    let mut user_retry_count = "retries:".to_string();
    let mut user_secret = "secret:".to_string();

    user_pin.push_str(&payload.username);
    user_retry_count.push_str(&payload.username);
    user_secret.push_str(&payload.username);


    let write_user_pin_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec![user_pin.into_bytes()]
    };

    let result = match send(vec![write_user_pin_op], false, &data).await {
        Ok(response) => response,
        Err(e) => return e,
    };

    if pin.into_bytes() != result[0] {
        let increment_user_pin_op = ProtoTransactionOp {
            op_type: pft::proto::execution::ProtoTransactionOpType::Increment.into(),
            operands: vec![user_retry_count.into_bytes()]
        };

        let increment_user_pin_result = match send(vec![increment_user_pin_op],false, &data).await {
            Ok(response) => response[0].clone(),
            Err(e) => return e,
        };

        return HttpResponse::Unauthorized().json(serde_json::json!({
            "message": "incorrect pin",
            "pin": payload.pin.clone(),
            "pinpin":payload.pin.clone().into_bytes(),
            "real pin": result[0],
            "retries": increment_user_pin_result,
        }))
    }

    let get_user_secret_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec![user_secret.into_bytes()]
    };

    let get_user_secret_result = match send(vec![get_user_secret_op], false, &data).await {
        Ok(response) => response[0].clone(),
        Err(e) => return e,
    };

    let user_secret = match String::from_utf8(get_user_secret_result) {
        Ok(user_secret) => user_secret,
        Err(e) => return HttpResponse::InternalServerError().body("Invalid UTF-8 data"),
    };

    HttpResponse::Ok().json(serde_json::json!({
        "message": "user secret store",
        "user secret": user_secret,
    }))
}

#[post("/toggle_byz_wait")]
async fn toggle_byz_wait(data: web::Data<AppState>) -> impl Responder {
    let mut state = data.probe_for_byz_commit.load(Ordering::SeqCst);
    state = !state;
    data.probe_for_byz_commit.store(state, Ordering::SeqCst);
    HttpResponse::Ok().json(serde_json::json!({
        "message": "byzantine wait toggled",
        "probe_for_byz_commit": state,
    }))
}

#[get("/")]
async fn home(_data: web::Data<AppState>) -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "message": "hello from a non-shared, local state",
    }))
}

pub async fn run_actix_server(config: Config, batch_proposer_tx: pft::utils::channel::AsyncSenderWrapper<TxWithAckChanTag>, actix_threads: usize) -> std::io::Result<()> {
    let addr = config.net_config.addr.clone();
    // Add 1000 to the port.
    let (host, port) = addr.split_once(':').unwrap();
    let port: u16 = port.parse().unwrap();
    let port = port + 1000;
    let addr = format!("{}:{}", host, port);

    let batch_size = config.consensus_config.max_backlog_batch_size.max(256);


    let probe_for_byz_commit = Arc::new(AtomicBool::new(false)); // This is a global state!

    HttpServer::new(move || {
        // Each worker thread creates its own client instance.
        let state = AppState {
            batch_proposer_tx: batch_proposer_tx.clone(),
            probe_for_byz_commit: probe_for_byz_commit.clone(),
            curr_client_tag: AtomicU64::new(0),
        };

        App::new()
            .app_data(web::Data::new(state))
            .service(home)
            .service(auth)
            .service(storeSecret)
            .service(recoverSecret)
            .service(toggle_byz_wait)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await?;
    Ok(())
}

async fn send(transaction_ops: Vec<ProtoTransactionOp>, isRead: bool, state: &AppState) -> Result<Vec<Vec<u8>>, HttpResponse> {
    let transaction_phase = ProtoTransactionPhase {
        ops: transaction_ops,
    };

    let transaction = if isRead {
        ProtoTransaction {
            on_receive: Some(transaction_phase.clone()),
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
            is_2pc: false,
        }
    } else {
        ProtoTransaction {
            on_receive: None,
            on_crash_commit: Some(transaction_phase.clone()),
            on_byzantine_commit: None,
            is_reconfiguration: false,
            is_2pc: false,
        }
    };
    
    let current_tag = state.curr_client_tag.fetch_add(1, Ordering::AcqRel);

    let (tx, mut rx) = mpsc::channel(1);
    let tx_with_ack_chan_tag: TxWithAckChanTag = (Some(transaction), (tx, current_tag, SenderType::Anon));
    state.batch_proposer_tx.send(tx_with_ack_chan_tag).await.unwrap();

    let (resp, _) = match rx.recv().await {
        Some(resp) => resp,
        None => {
            return Err(HttpResponse::InternalServerError().body("Error receiving response"));
        }
    };

    let resp = resp.as_ref();
    let decoded_payload = match ProtoClientReply::decode(&resp.0.as_slice()[0..resp.1]) {
        Ok(payload) => payload,
        Err(e) => {
            warn!("Error decoding response: {}", e);
            return Err(HttpResponse::InternalServerError().body("Error decoding response"));
        }
    };

    let mut result: Vec<Vec<u8>> = Vec::new();
    match decoded_payload.reply.unwrap() {
        pft::proto::client::proto_client_reply::Reply::Receipt(receipt) => {
            if let Some(tx_result) = receipt.results {
                if tx_result.result.is_empty() {
                    return Ok(result);
                }
                for op_result in tx_result.result {
                    for value in op_result.values {
                        result.push(value);
                    }
                }
            }
        },
        _ => {
            return Err(HttpResponse::NotFound().json(serde_json::json!({
                "message": "error, no Receipt found",
                "result": result,
            })))
        },
    };
    Ok(result)
}

async fn authenticate_user(
    username: String,
    password: String,
    data: &AppState,
) -> Result<bool, HttpResponse> {
    let transaction_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec![username.clone().into_bytes()],
    };

    let result = match send(vec![transaction_op], true, data).await {
        Ok(response) => response,
        Err(e) => return Err(e),
    };

    if result.is_empty() {
        return Err(HttpResponse::NotFound().json(serde_json::json!({
            "message": "username does not exist",
            "user": username,
        })));
    }

    if hash(&password.into_bytes()) != result[0] {
        return Err(HttpResponse::Unauthorized().json(serde_json::json!({
            "message": "incorrect password",
            "user": username,
        })));
    }
    Ok(true)
}

/*
Example usage:
curl -X POST "http://localhost:8080/auth" -H "Content-Type: application/json" -d '{"username":"teddy", "password":"hi"}'

*/
