use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use bitcode::decode;
use crossbeam::deque::Worker;
use ed25519_dalek::pkcs8::spki::der::asn1::SetOfVec;
use gluesql::core::ast_builder::function::sign;
use gluesql::core::sqlparser::keywords::USER;
use log::{debug, warn};
use nix::libc::passwd;
use prost::Message;
use serde::Deserialize;
use sha2::digest::typenum::Integer;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Instant;


use pft::config::Config;
use pft::crypto::{KeyStore, hash};
use pft::proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase};
use pft::rpc::client::Client;
use pft::rpc::{MessageRef, PinnedMessage};
use pft::{config::ClientConfig, proto::{client::{self, ProtoClientReply, ProtoClientRequest}, rpc::ProtoPayload}, rpc::client::PinnedClient, utils::channel::{make_channel, Receiver, Sender}};
use crate::payloads::{RegisterPayload, PubKeyPayload};

use rand_chacha::ChaCha20Rng;
use ed25519_dalek::{ed25519, PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH, SigningKey, SecretKey, VerifyingKey};
#[derive(Clone)]
struct AppState {
    client: Arc<PinnedClient>,
    node_list: Vec<String>,
    curr_leader_id: Arc<Mutex<usize>>,
    curr_round_robin_id: Arc<Mutex<usize>>,
    curr_client_tag:  Arc<Mutex<usize>>
}


#[post ("/register")]
async fn register(payload: web::Json<RegisterPayload>, data: web::Data<AppState>) -> impl Responder {
    let username = payload.username.clone();
    let password = payload.password.clone();
    let client = &data.client;
    let client_tag = &data.curr_client_tag;

    //query kms for username
    let transaction_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec![username.clone().into_bytes()],
    }; 

    let result = match send(vec![transaction_op], client, &data.curr_client_tag).await {
        Ok(response) => response,
        Err(e) => return e
    }; //add client tag

   if !result.is_empty() {
    return HttpResponse::Conflict().json(serde_json::json!({
        "message": "username already exists",
    }))
   }
    //assume at this point username does not exist --> create new username and password
    let create_user_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Write.into(),
        operands: vec![username.clone().into_bytes(), hash(&password.clone().into_bytes())],
    };

    let create_user_result = match send(vec![create_user_op], client, &data.curr_client_tag).await {
        Ok(response) => response,
        Err(e) => return e
    }; //add client tag

    //add user to list of users
    let get_user_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec!["user".as_bytes().to_vec()],
    };

    let get_user_result = match send(vec![get_user_op], client, &data.curr_client_tag).await {
        Ok(response) => response,
        Err(e) => return e
    }; //add client tag

    
    let mut users: Vec<String> = Vec::new();   
    if !get_user_result.is_empty() {
        users = serde_json::from_slice(&get_user_result).expect("Deserialization failed");
    }
    users.push(username.clone());
    let serialized_users: Vec<u8> = serde_json::to_vec(&users).expect("Serialization failed");

    let update_users_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Write.into(),
        operands: vec!["user".as_bytes().to_vec(), serialized_users],
    };

    let update_users_result = match send(vec![update_users_op], client, &data.curr_client_tag).await {
        Ok(response) => response,
        Err(e) => return e
    }; //add client tag

    HttpResponse::Ok().json(serde_json::json!({
        "message": "user created",
        "users": users,
    }))
}


// POST /key → Retrieves the user's private key, supports attestation and key wrapping.
// POST /refresh → Generates a new key pair and stores it.
// GET /pubkey → Retrieves a public key by kid.
// GET /listpubkeys → Lists all public keys.

#[post ("/refresh")]
async fn refresh(payload: web::Json<RegisterPayload>, data: web::Data<AppState>) -> impl Responder {
    let client = &data.client;
    let mut csprng = rand::rngs::OsRng;
    let signing_key: SigningKey = SigningKey::generate(&mut csprng);

    match authenticate_user(payload.username.clone(), payload.password.clone(), client,  &data.curr_client_tag).await {
        Ok(valid) => valid,
        Err(e) => return e
    };

    let private_key = signing_key.verifying_key().to_bytes();
    let public_key = signing_key.to_bytes();
    
    let mut public_insert_key = "pub:".to_string();
    public_insert_key.push_str(&payload.username);
    
    let mut priv_insert_key = "priv:".to_string();
    priv_insert_key.push_str(&payload.username);


    let write_pub_key_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Write.into(),
        operands: vec![ public_insert_key.into_bytes(), public_key.to_vec()],
    };

    let write_priv_key_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Write.into(),
        operands: vec![priv_insert_key.into_bytes(), private_key.to_vec()],
    };

    let result = match send(vec![write_pub_key_op, write_priv_key_op], client, &data.curr_client_tag).await {
        Ok(response) => response,
        Err(e) => return e
    }; //add client tag
    
    HttpResponse::Ok().json(serde_json::json!({
        "message": "created key pair for user",
        "username": &payload.username,
        "public key": public_key,
        "private key": private_key,
    }))
}

#[get("/listpubkeys")]
async fn listpubkeys(data: web::Data<AppState>) -> impl Responder {
    let client = &data.client;

    let get_user_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec!["user".as_bytes().to_vec()],
    };

    let get_user_result = match send(vec![get_user_op],client, &data.curr_client_tag).await {
        Ok(response) => response,
        Err(e) => return e
    }; //add client tag

    let mut users: Vec<String> = Vec::new();   
    if !get_user_result.is_empty() {
        users = serde_json::from_slice(&get_user_result).expect("Deserialization failed");
    }
    let mut user_ops = Vec::new();
    //get public key of each user
    for user in users {
        let mut key = "pub:".to_string();
        key.push_str(&user);

        let get_user_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec![key.into_bytes()],
        };

        user_ops.push(get_user_op);
    }
    
    let mut public_keys = Vec::new();
    for op in user_ops {
        let user_public_key_result = match send(vec![op], client, &data.curr_client_tag).await {
            Ok(response) => response,
            Err(e) => return e
        };

        let pub_key_arr:[u8; PUBLIC_KEY_LENGTH] = user_public_key_result.try_into().expect("Vec has incorrect length");
        public_keys.push(pub_key_arr);
    }
    
    HttpResponse::Ok().json(serde_json::json!({
        "message": "hi",
        "public keys": public_keys
    }))
}

#[get("/pubkey")]
async fn pubkey(payload: web::Json<PubKeyPayload>, data: web::Data<AppState>) -> impl Responder {
    let client = &data.client;

    let mut key = "pub:".to_string();
    key.push_str(&payload.username);

    let transaction_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec![key.into_bytes()],
    }; 

    let user_public_key_result = match send(vec![transaction_op],client, &data.curr_client_tag).await {
        Ok(response) => response,
        Err(e) => return e
    };
    
    if user_public_key_result.is_empty() {
        return HttpResponse::NotFound().json(serde_json::json!({
            "message": "public key could not be found",
            "username": &payload.username,
        }))
    }
    
    let pub_key_arr:[u8; PUBLIC_KEY_LENGTH] = user_public_key_result.try_into().expect("Vec has incorrect length");

    let mut tag_guard = data.curr_client_tag.lock().await;
    *tag_guard += 0;
    let current_tag = *tag_guard as u64;

    HttpResponse::Ok().json(serde_json::json!({
        "message": "public key of user",
        "username": &payload.username,
        "public key": pub_key_arr,
        "client tag": (current_tag) as u64,
    }))
}

#[get("/privkey")]
async fn privkey(payload: web::Json<RegisterPayload>, data: web::Data<AppState>) -> impl Responder {
    let client = &data.client;


    match authenticate_user(payload.username.clone(), payload.password.clone(), client, &data.curr_client_tag).await {
        Ok(valid) => valid,
        Err(e) => return e
    };

    let mut key = "priv:".to_string();
    key.push_str(&payload.username);

    let transaction_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec![key.into_bytes()],
    }; 

    let user_priv_key_result = match send(vec![transaction_op], client, &data.curr_client_tag).await {
        Ok(response) => response,
        Err(e) => return e
    };

    if user_priv_key_result.is_empty() {
        return HttpResponse::NotFound().json(serde_json::json!({
            "message": "private key could not be found",
            "username": &payload.username,
        }))
    }
    
    let priv_key_arr:[u8; SECRET_KEY_LENGTH] = user_priv_key_result.try_into().expect("Vec has incorrect length");

    HttpResponse::Ok().json(serde_json::json!({
        "message": "private key of user",
        "username": &payload.username,
        "private key": priv_key_arr
    }))
}

#[get("/")]
async fn home(data: web::Data<AppState>) -> impl Responder {
    let nodes = data.node_list.clone();
    HttpResponse::Ok().json(serde_json::json!({
        "message": "hi",
        "data": nodes
    }))
}

pub async fn run_actix_server(config: Config) -> std::io::Result<()> {
    let curr_leader_id = Arc::new(Mutex::new(0));
    let curr_round_robin_id=  Arc::new(Mutex::new(0));
    let curr_client_tag=  Arc::new(Mutex::new(0));


    let mut keys = KeyStore::empty();
    keys.priv_key = KeyStore::get_privkeys(&config.rpc_config.signing_priv_key_path);
    let keys = keys.clone();

    let client = Arc::new(Client::new(&config, &keys, false, 0 as u64).into());

    let _ = HttpServer::new(move || {
        App::new()
        .app_data(web::Data::new(AppState {
                client: client.clone(),
                node_list: config.consensus_config.node_list.to_vec(),
                curr_leader_id: curr_leader_id.clone(),
                curr_round_robin_id: curr_round_robin_id.clone(),
                curr_client_tag: curr_client_tag.clone(),
            }
        ))
        .service(register)
        .service(pubkey)
        .service(listpubkeys)
        .service(refresh)
        .service(privkey)
        .service(home)
    })
    .bind("127.0.0.1:8080")? 
    .run()                
    .await;
    Ok(())
}

async fn send(transaction_ops: Vec<ProtoTransactionOp>, client:&Arc<PinnedClient>,  client_tag: &Arc<Mutex<usize>>) -> Result<Vec<u8>, HttpResponse> {
    let transaction_phase = ProtoTransactionPhase {
        ops: transaction_ops,
    };

    let transaction = ProtoTransaction {
        on_receive:None,
        on_crash_commit: Some(transaction_phase.clone()),
        on_byzantine_commit: None,
        is_reconfiguration: false,
    };

    let mut tag_guard = client_tag.lock().await;
    *tag_guard += 1;
    let current_tag = *tag_guard as u64;

    let rpc_msg_body = ProtoPayload {
        message: Some(pft::proto::rpc::proto_payload::Message::ClientRequest(ProtoClientRequest {
            tx: Some(transaction),
            origin: "name".to_string(), //change? doesn't really matter
            sig: vec![0u8; 1],
            client_tag: current_tag,
        })),
    };
    

    let mut buf = Vec::new();
    let sz = buf.len();

    if let Err(e) = rpc_msg_body.encode(&mut buf) {
        warn!("Error encoding request: {}", e);
    }

    let sz = buf.len();
    let request = PinnedMessage::from(buf, sz, pft::rpc::SenderType::Anon);

    let resp = match PinnedClient::send_and_await_reply(client, &"node1".to_string(), request.as_ref()).await {
        Ok(resp) => resp,
        Err(e) => {
            warn!("Error sending request: {}", e);
            return Err(HttpResponse::InternalServerError().body(format!("Error sending request: {}", e)));
        }
    };

    let resp = resp.as_ref();
    let mut result: Vec<u8> = Vec::new();
    let decoded_payload  = match ProtoClientReply::decode(&resp.0.as_slice()[0..resp.1]) {
        Ok(payload) => payload,
        Err(e) => {
            warn!("Error decoding response: {}", e);
            return Err(HttpResponse::InternalServerError().body("Error decoding response"));
        }
    };

    match decoded_payload.reply.unwrap() {
        pft::proto::client::proto_client_reply::Reply::Receipt(receipt) => {
            if let Some(tx_result) = receipt.results {
                if tx_result.result.is_empty()  {
                    return Ok(result);
                }
                for op_result in tx_result.result {
                    for value in op_result.values {
                        result = value; //not beautiful, fix later.
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

async fn authenticate_user(username: String, password: String, client:&Arc<PinnedClient>, client_tag: &Arc<Mutex<usize>>) -> Result<bool, HttpResponse> {
    let transaction_op = ProtoTransactionOp {
        op_type: pft::proto::execution::ProtoTransactionOpType::Read.into(),
        operands: vec![username.clone().into_bytes()],
    };

    let result = match send(vec![transaction_op],  client, client_tag,).await {
        Ok(response) => response,
        Err(e) => return Err(e)
    }; //add client tag

    if result.is_empty() {
        return Err(HttpResponse::NotFound().json(serde_json::json!({
            "message": "username does not exist",
            "user": username,
        })))
    };
        
    //check hash(password) matches
    if hash(&password.clone().into_bytes()) != result {
        return Err(HttpResponse::Unauthorized().json(serde_json::json!({
            "message": "incorrect password",
            "user": username,
        })));
    }
    Ok(true)
}

// Example
/*
Register User
curl -X POST "http://localhost:8080/register" -H "Content-Type: application/json" -d '{"username":"teddy", "password":"hi"}'

Create key value pair
curl -X POST "http://localhost:8080/refresh" -H "Content-Type: application/json" -d '{"username":"teddy", "password":"hi"}'

View public key
curl -X GET "http://localhost:8080/pubkey" -H "Content-Type: application/json" -d '{"username":"teddy"}'

View private key
curl -X GET "http://localhost:8080/privkey" -H "Content-Type: application/json" -d '{"username":"teddy", "password":"hi"}'

Create second user
curl -X GET "http://localhost:8080/register" -H "Content-Type: application/json" -d '{"username":"teddy2", "password":"hi"}'

Create second user's key
curl -X POST "http://localhost:8080/refresh" -H "Content-Type: application/json" -d '{"username":"teddy2", "password":"hi"}'

List all public keys
curl -X GET "http://localhost:8080/listpubkeys" -H "Content-Type: application/json"
*/




