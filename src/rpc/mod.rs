pub mod server;
pub mod client;
pub mod auth;

/// Do not run these tests all together.
/// The tests needs a config directory.
/// Create it by running: sh scripts/gen_local_config.sh configs 7 scripts/local_template.json
#[cfg(test)]
mod tests;