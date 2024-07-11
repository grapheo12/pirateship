# Local setup

Install Rust, jq, clang, llvm, cmake and protoc.
On an Ubuntu machine you can just run `sudo make install-deps-ubuntu` to install all dependencies.

Run:
```bash
sh scripts/gen_local_config.sh configs 3 scripts/local_template.json scripts/local_client_template.json
```
to create a 3 node local config in the `configs` directory with a bunch of defaults set in `scripts/local_template.json`.

The TLS certificates use `node*.localhost` as the Domain name.
Make sure `node*.localhost` resolves to `127.0.0.1` in your dev machines.
Azure machines using Ubuntu 20.04 already do this by default.
If it doesn't work, append the following line in `/etc/hosts` for `$i` in 1..7 inclusive:
```
127.0.0.1   node$i.localhost
```

Build the project using `make` and run as follows:

```bash
sh scripts/run_local.sh <NUM_NODES> <NUM_SECONDS> <CONFIG_DIR> <LOG_DIR>
```

For example,
```bash
sh scripts/run_local.sh 3 20 configs logs
```
will run the 3 node setup for which the configs are in `configs` for 20 seconds and dump all the logs in `logs`.


# TODO

(To do once a basic code structure is ready)
- [x] Test if a dedicated async task per connected server (with channels to pass data) is better than locked sockets.
- [ ] Create a good proto representation for transactions.