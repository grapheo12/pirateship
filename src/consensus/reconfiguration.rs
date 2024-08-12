use log::{error, info};
use nix::sys::signal;
use nix::sys::signal::Signal::SIGINT;
use nix::unistd::Pid;

/// Gracefully shut down the node
pub async fn do_graceful_shutdown() {
    info!("Attempting graceful shutdown");
    // The following works only on UNIX systems.
    let pid = Pid::this();
    match signal::kill(pid, SIGINT) {
        Ok(_) => {
            info!("Sent SIGINT to self. Shutting down.");
        },
        Err(e) => {
            error!("Failed to send SIGINT to self. Error: {:?}", e);
            std::process::exit(0);
        }
    }
}