use actix_web::{put, get, web, App, HttpResponse, HttpServer, Responder};
use std::sync::{Arc, Mutex};

use crate::proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase};
use crate::utils::channel::{make_channel, Receiver, Sender};



#[put("/set/{key}")]
async fn set_key(key: web::Path<String>, value: web::Json<String>) -> impl Responder {
    let key_str = key.into_inner();
    let value_str = value.into_inner();

    HttpResponse::Ok().json(serde_json::json!({
        "message": "Key set successfully",
        "key": key_str,
        "value": value_str
    }))
}

#[get("/get/{key}")]
async fn get_key(key: web::Path<String>) -> impl Responder {
    let key_str = key.into_inner();
    HttpResponse::Ok().json(serde_json::json!({
        "message": "Key found successfully",
        "key": key_str,
    }))
}

#[get("/")]
async fn home() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "message": "Welcome to the Pirateship API!"
    }))
}

pub async fn run_server() -> std::io::Result<()> {
    HttpServer::new( || {
        App::new()
            .service(set_key)
            .service(get_key)
            .service(home)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

// Tests
/*
curl -X GET "http://localhost:8080/"

curl -X GET "http://localhost:8080/get/username"

curl -X PUT "http://localhost:8080/set/username" -H "Content-Type: application/json" -d '"john_doe"'
*/

