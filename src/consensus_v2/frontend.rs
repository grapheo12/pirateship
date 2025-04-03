    use actix_web::{put, get, web, App, HttpResponse, HttpServer, Responder};
    use bitcode::decode;
    use crossbeam::deque::Worker;
    use log::{debug, warn};
    use prost::Message;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use std::time::Instant;


    use crate::client::workload_generators::Executor;
    use crate::config::Config;
    use crate::crypto::KeyStore;
    use crate::proto::checkpoint::{ProtoBackFillRequest, ProtoBackFillResponse};
    use crate::proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase};
    use crate::rpc::client::Client;
    use crate::rpc::{MessageRef, PinnedMessage};
    use crate::{config::ClientConfig, proto::{client::{self, ProtoClientReply, ProtoClientRequest}, rpc::ProtoPayload}, rpc::client::PinnedClient, utils::channel::{make_channel, Receiver, Sender}};

    #[derive(Clone)]
    struct AppState {
        client: Arc<PinnedClient>,
        node_list: Vec<String>, //make into reference?
        curr_leader_id: Arc<Mutex<usize>>,
        curr_round_robin_id: Arc<Mutex<usize>>
    }

    #[put("/set/{key}")]
    async fn set_key(key: web::Path<String>, value: web::Json<String>, data: web::Data<AppState>) -> impl Responder {
        let key_str = key.into_inner();
        let value_str = value.into_inner();
        let client = &data.client;
        let node_list = &data.node_list;
        let mut curr_leader_id: usize = {
            let guard = data.curr_leader_id.lock().await;
            *guard // dereference the guard to get the inner usize value
        };
        
        let mut curr_round_robin_id: usize = {
            let guard = data.curr_round_robin_id.lock().await;
            *guard
        };


        let transaction_op = ProtoTransactionOp {
            op_type: crate::proto::execution::ProtoTransactionOpType::Write.into(),
            operands: vec![key_str.clone().into_bytes(), value_str.clone().into_bytes()],
        };

        let transaction_phase = ProtoTransactionPhase {
            ops: vec![transaction_op.clone()],
        };

        let transaction = ProtoTransaction {
            on_receive:None,
            on_crash_commit: Some(transaction_phase.clone()),
            on_byzantine_commit: None,
            is_reconfiguration: false,
        };

        // let client_request = ProtoPayload {
        //     message: Some(crate::proto::rpc::proto_payload::Message::ClientRequest(ProtoClientRequest {
        //         tx: Some(transaction),
        //         origin: "name".to_string(), //temp
        //         sig: vec![0u8; 1],
        //         client_tag: (0 + 1) as u64, //temp
        //     }))
        // };

        // let mut req = OutstandingRequest::default();
        // req.id = (0 + 1) as u64;
        // req.payload = client_request.encode_to_vec();

        let rpc_msg_body = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::ClientRequest(ProtoClientRequest {
                tx: Some(transaction),
                origin: "name".to_string(), //temp
                sig: vec![0u8; 1],
                client_tag: (0 + 1) as u64, //temp
            })),
        };
        
        let mut buf = Vec::new();
        let sz = buf.len();

        // Try to encode the request payload into a buffer.
        if let Err(e) = rpc_msg_body.encode(&mut buf) {
            warn!("Error encoding request: {}", e);
        }

        // Construct the request message.
        let sz = buf.len();
        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

        // Send the request and await the reply.
        let resp = match PinnedClient::send_and_await_reply(&data.client, &"node1".to_string(), request.as_ref()).await {
            Ok(resp) => resp,
            Err(e) => {
                warn!("Error sending request: {}", e);
                return HttpResponse::InternalServerError().body(format!("Error sending request: {}", e));
            }
        };

        let resp = resp.as_ref();

        // Decode the response payload.
        let decoded_payload  = match ProtoClientReply::decode(&resp.0.as_slice()[0..resp.1]) {
            Ok(payload) => payload,
            Err(e) => {
                warn!("Error decoding response: {}", e);
                return HttpResponse::InternalServerError().body("Error decoding response");
            }
        };

        // Return the HTTP response including any desired data.
        HttpResponse::Ok().json(serde_json::json!({
            "message": "Key set successfully",
            "key": key_str,
            "value": value_str,
            "node list": data.node_list,
            "payload": decoded_payload
        }))
    }

    #[get("/get/{key}")]
    async fn get_key(key: web::Path<String>) -> impl Responder {
        let key_str = key.into_inner();

        ProtoTransactionOp {
            op_type: crate::proto::execution::ProtoTransactionOpType::Read.into(),
            operands: vec![key_str.clone().into_bytes()],
        };

        HttpResponse::Ok().json(serde_json::json!({
            "message": "Key found successfully",
            "key": key_str,
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

    pub async fn setup_frontend(config: Config) -> String {
        // let _ = run_actix_server(config).await;
        "hello".to_string()
    }

    pub async fn test_actix_server() -> std::io::Result<()> {
        HttpServer::new(|| {
            App::new()
        })
        .bind("127.0.0.1:8080")? 
        .run()                
        .await
    }

    pub async fn run_actix_server(config: Config) -> std::io::Result<()> {
        let curr_leader_id = Arc::new(Mutex::new(0));
        let curr_round_robin_id=  Arc::new(Mutex::new(0));

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
                    curr_round_robin_id: curr_round_robin_id.clone()
                }
            ))
            .service(set_key)
            .service(get_key)
            .service(home)
        })
        .bind("127.0.0.1:8080")? 
        .run()                
        .await;
        Ok(())
    }

    struct OutstandingRequest {
        id: u64,
        payload: Vec<u8>,
        executor_mode: Executor,
        last_sent_to: String,
        start_time: Instant
    }

    impl OutstandingRequest {
        pub fn default() -> Self {
            Self {
                id: 0,
                payload: Vec::new(),
                last_sent_to: String::new(),
                start_time: Instant::now(),
                executor_mode: Executor::Any,
            }
        }
    }

    // async fn send_request(client: &PinnedClient, req: &mut OutstandingRequest, node_list: &Vec<String>, curr_leader_id: &mut usize, curr_round_robin_id: &mut usize) {
    //     let buf = &req.payload;
    //     let sz = buf.len();
    //     loop {
    //         let res = match req.executor_mode {
    //             Executor::Leader => {
    //                 let curr_leader = &node_list[*curr_leader_id];
    //                 req.last_sent_to = curr_leader.clone();

                    
    //                 PinnedClient::send(
    //                     client,
    //                     &curr_leader,
    //                     MessageRef(buf, sz, &crate::rpc::SenderType::Anon),
    //                 ).await
    //             },
    //             Executor::Any => {
    //                 let recv_node = &node_list[(*curr_round_robin_id) % node_list.len()];
    //                 *curr_round_robin_id = *curr_round_robin_id + 1;
    //                 req.last_sent_to = recv_node.clone();

    //                 PinnedClient::send(
    //                     client,
    //                     recv_node,
    //                     MessageRef(&buf, buf.len(), &crate::rpc::SenderType::Anon),
    //                 ).await
    //             },
    //         };

    //         if res.is_err() {
    //             debug!("Error: {:?}", res);
    //             *curr_leader_id = (*curr_leader_id + 1) % node_list.len();
    //             // outstanding_requests.clear(); // Clear it out because I am not going to listen on that socket again
    //             // info!("Retrying with leader {} Backoff: {} ms: Error: {:?}", curr_leader, current_backoff_ms, res);
    //             // backoff!(*current_backoff_ms);
    //             continue;
    //         }
    //         break;
    //     }
    // }
    // Tests
    /*
    curl -X GET "http://localhost:8080/"

    curl -X GET "http://localhost:8080/get/username"

    curl -X PUT "http://localhost:8080/set/username" -H "Content-Type: application/json" -d '"john_doe"'
    */

