# Local setup

Install Rust, jq, Python (with pip and virtualenv), clang, llvm, cmake and protoc.
On an Ubuntu machine you can just run `sudo make install-deps-ubuntu` to install all dependencies.

To build, just run `make`.

To run a setup, you need to have a `nodelist.txt` with a tab separated value of node names and IP addresses as follows:
```tsv
clientpool_vm0  10.2.7.5
clientpool_vm1  10.2.7.4
nodepool_vm0    10.2.6.7
nodepool_vm1    10.2.6.4
nodepool_vm2    10.2.6.5
nodepool_vm3    10.2.6.6
```

Machines whose name starts with `client` will be used as client machines and nodes whose name starts with `node` will be used a consensus nodes.
These IP addresses NEED NOT be unique.
If you want to reuse a machine to simulate more number of nodes (or want to perform local testing),
feel free to copy the IP addresses of existing machines (or use `127.0.0.1`) and use a new name.

To be compliant with the terraform script that I generally use to create test VMs,
you need to have an user called `azureadmin` in each of these VMs.
Further, one must be able to SSH into `azureadmin@ip` using a common SSH key.
Generate and save this key as `cluster_key.pem`.

Save the `nodelist.txt` and `cluster_key.pem` and copy the path onto `scripts/run_all_protocols.sh`.

Now you can an experiment with all protocols using:

```bash
virtualenv scripts/venv
source scripts/venv/bin/activate
pip install -r scripts/requirements.txt

sh scripts/run_all_protocols.sh
```

# Current status

![Performance of Cochin wrt other protocols; Non-TEE and LAN setup](perf.png)


# TODO

- [ ] **Reconfiguration.**
    * Verifying signatures from old configs in terms of the new config
    * Reconf on byz commit, so that all correct replicas eventually (after GST) knows of this new config and joins it.
    * Until then it is possible to crash commit 2 entries by 2 leaders in 2 different configs.
    * But that's ok. Byz commit still won't happen.
    
- [ ] Implication of handling multiple platforms?
- [ ] Handling persistence: Same disk or replicated disks?
- [ ] Why is diverse_raft performing worse than cochin?