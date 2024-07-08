# Local setup

Install Rust and jq.

Run:
```bash
sh scripts/gen_local_config.sh configs 7 scripts/local_template.json
```
to create a 7 node local config in the `configs` directory with a bunch of defaults set in `scripts/local_template.json`.

The TLS certificates use `node*.localhost` as the Domain name.
Make sure `node*.localhost` resolves to `127.0.0.1` in your dev machines.
Azure machines using Ubuntu 20.04 already do this by default.
If it doesn't work, append the following line in `/etc/hosts` for `$i` in 1..7 inclusive:
```
127.0.0.1   node$i.localhost
```

Build the project using `cargo build` and run as follows:

```bash
RUST_LOG=debug ./target/debug/pft configs/node$i.json
```
