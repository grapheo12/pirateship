use actix_web::{put, get, web, App, HttpResponse, HttpServer, Responder};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use pft::consensus_v2::engines::kvs;

type Store = Arc<Mutex<HashMap<String, String>>>;

#[put("/set/{key}")]
async fn set_key(store: web::Data<Store>, key: web::Path<String>, value: web::Json<String>) -> impl Responder {
    let key_str = key.into_inner();
    let value_str = value.into_inner();

    HttpResponse::Ok().json(serde_json::json!({
        "message": "Key set successfully",
        "key": key_str,
        "value": value_str
    }))
    
}

#[get("/get/{key}")]
async fn get_key(store: web::Data<Store>, key: web::Path<String>) -> impl Responder {
    let key_str = key.into_inner();
    HttpResponse::Ok().json(serde_json::json!({
        "message": "Key found successfully",
        "key": key_str,
    }))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {


    HttpServer::new(move || {
        App::new()
            .service(set_key)
            .service(get_key)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

//tests
/*
curl -X GET "http://localhost:8080/get/username"

curl -X PUT "http://localhost:8080/set/username" -H "Content-Type: application/json" -d '"john_doe"'

*/

