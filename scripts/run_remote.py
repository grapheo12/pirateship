import os
from fabric import Connection
from fabric.runners import Result
from typing import List, Tuple, OrderedDict, Dict
import invoke
import click
from gen_cluster_config import gen_config, CONFIG_SUFFIX
import collections
import datetime
import json
import time

# Run a sequence of commands
# Crash if any one command fails.
# Return true if all commands succeeded
def run_all(cmds: List[str], conn: Connection) -> Tuple[bool, List[str]]:
    outputs = []
    for cmd in cmds:
        try:
            # The join() from asynchronous=True is blocking.
            # Fabric doesn't play nice with asyncio
            res: Result = conn.run(cmd, hide=True, pty=True)
            outputs.append(res.stdout)
        except Exception as e:
            print(f"[WARN] Error running command: {e}")
            return False, outputs
    else:
        return True, outputs
    
def get_current_git_hash():
    res = invoke.run("git rev-parse HEAD", hide=True) # type: ignore
    assert not(res is None)
    return res.stdout.strip()


def build_project():
    res = invoke.run("make") # type: ignore
    assert not(res is None)
    return res.stdout.strip()

# This ordering must match with the one generated by gen_cluster_nodelist.
# We make sure this happens by re-reading the same file for ip list.
# And returning an ordered dict
def tag_all_machines(ip_list) -> Tuple[OrderedDict[str, str], OrderedDict[str, str]]:
    nodelist = collections.OrderedDict()
    clientlist = collections.OrderedDict()
    node_cnt = 0
    client_cnt = 0
    with open(ip_list) as f:
        for line in f.readlines():
            # Terraform generates VM names as `nodepool_vm0` and `clientpool_vm0`.
            # IP list output by terraform must be of the form:
            # nodepool_vm0 <private ip address> OR
            # clientpool_vm0 <private ip address>
            if line.startswith("node"):
                node_cnt += 1
                ip = line.split()[1]
                nodelist["node" + str(node_cnt)] = ip.strip()
            
            if line.startswith("client"):
                client_cnt += 1
                ip = line.split()[1]
                clientlist["client" + str(client_cnt)] = ip.strip()

    return nodelist, clientlist

def create_dirs_and_copy_files(node_conns, client_conns, wd, repeat, git_hash):
    for node, conn in node_conns.items():
        run_all([
            f"mkdir -p pft/{wd}",
            f"echo '{git_hash}' > pft/{wd}/git_hash.txt",
            f"mkdir -p pft/{wd}/target/release",
            f"mkdir -p pft/{wd}/configs"
        ] + [f"mkdir -p pft/{wd}/logs/{i}" for i in range(repeat)], conn)


        with open(f"configs/{node}{CONFIG_SUFFIX}") as f:
            cfg = json.load(f)
        

        conn.put(f"{cfg['net_config']['tls_cert_path']}", remote=f"pft/{wd}/configs/")
        conn.put(f"{cfg['net_config']['tls_key_path']}", remote=f"pft/{wd}/configs/")
        conn.put(f"{cfg['net_config']['tls_root_ca_cert_path']}", remote=f"pft/{wd}/configs/")
        conn.put(f"{cfg['rpc_config']['allowed_keylist_path']}", remote=f"pft/{wd}/configs/")
        conn.put(f"{cfg['rpc_config']['signing_priv_key_path']}", remote=f"pft/{wd}/configs/")
        conn.put(f"configs/{node}{CONFIG_SUFFIX}", remote=f"pft/{wd}/configs/")
        conn.put("target/release/server", remote=f"pft/{wd}/target/release")

    for client, conn in client_conns.items():
        run_all([
            f"mkdir -p pft/{wd}",
            f"echo '{git_hash}' > pft/{wd}/git_hash.txt",
            f"mkdir -p pft/{wd}/target/release",
            f"mkdir -p pft/{wd}/configs"
        ] + [f"mkdir -p pft/{wd}/logs/{i}" for i in range(repeat)], conn)

        
        with open(f"configs/{client}{CONFIG_SUFFIX}") as f:
            cfg = json.load(f)
        
        conn.put(f"{cfg['net_config']['tls_root_ca_cert_path']}", remote=f"pft/{wd}/configs/")
        conn.put(f"{cfg['rpc_config']['signing_priv_key_path']}", remote=f"pft/{wd}/configs/")

        conn.put(f"configs/{client}{CONFIG_SUFFIX}", remote=f"pft/{wd}/configs/")
        conn.put("target/release/client", remote=f"pft/{wd}/target/release")


def run_nodes(node_conns: Dict[str, Connection], repeat_num: int, wd: str) -> List:
    promises = []
    
    for node, conn in node_conns.items():
        prom = conn.run(f"cd pft/{wd} && ./target/release/server configs/{node}{CONFIG_SUFFIX} > logs/{repeat_num}/{node}.log 2> logs/{repeat_num}/{node}.err",
                 pty=True, asynchronous=True, hide=True)
        promises.append(prom)

    return promises



def run_clients(client_conns: Dict[str, Connection], repeat_num: int, wd: str) -> List:
    promises = []
    
    for client, conn in client_conns.items():
        prom = conn.run(f"cd pft/{wd} && ./target/release/client configs/{client}{CONFIG_SUFFIX} > logs/{repeat_num}/{client}.log 2> logs/{repeat_num}/{client}.err",
                 pty=True, asynchronous=True, hide=True)
        promises.append(prom)

    return promises



def kill_clients(client_conns: Dict[str, Connection]):
    for conn in client_conns.values():
        run_all([
            "pkill -c client"       # There better not be any other process that matches this.
        ], conn)


def kill_nodes(node_conns: Dict[str, Connection]):
    for conn in node_conns.values():
        run_all([
            "pkill -c server"       # There better not be any other process that matches this.
        ], conn)

def copy_log(name: str, conn: Connection, repeat_num: int, wd: str):
    conn.get(f"pft/{wd}/logs/{repeat_num}/{name}.log", local=f"logs/{wd}/{repeat_num}/")
    conn.get(f"pft/{wd}/logs/{repeat_num}/{name}.err", local=f"logs/{wd}/{repeat_num}/")

def copy_logs(node_conns, client_conns, repeat_num, wd):
    invoke.run(f"mkdir -p logs/{wd}/{repeat_num}", hide=True)
    for node, conn in node_conns.items():
        copy_log(node, conn, repeat_num, wd)

    for client, conn in client_conns.items():
        copy_log(client, conn, repeat_num, wd)


def run_remote(node_template, client_template, ip_list, identity_file, repeat, seconds):
    # build_project()
    git_hash = get_current_git_hash()
    gen_config("configs", "cluster", node_template, client_template, ip_list, -1)
    nodes, clients = tag_all_machines(ip_list)

    print("Creating SSH connections")
    node_conns = {node: Connection(
        host=ip,
        user="azureadmin", # This dependency comes from terraform
        connect_kwargs={
            "key_filename": identity_file
        }
    ) for node, ip in nodes.items()}

    
    client_conns = {client: Connection(
        host=ip,
        user="azureadmin", # This dependency comes from terraform
        connect_kwargs={
            "key_filename": identity_file
        }
    ) for client, ip in clients.items()}

    curr_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    print("Current working directory", curr_time)

    print("Copying files")
    create_dirs_and_copy_files(node_conns, client_conns, curr_time, repeat, git_hash)

    for i in range(repeat):
        print("Experiment sequence num:", i)

        print("Running Nodes")
        promises = []
        promises.extend(run_nodes(node_conns, i, curr_time))

        time.sleep(1)

        print("Running clients")
        promises.extend(run_clients(client_conns, i, curr_time))

        print("Running experiments for", seconds, "seconds")
        time.sleep(seconds)
        
        print("Killing clients")
        kill_clients(client_conns)

        print("Killing nodes")
        kill_nodes(node_conns)

        print("Joining on all promises (which should have terminated by now)")
        for prom in promises:
            try:
                prom.join()
            except Exception as e:
                print(e)

        print("Reestablishing connections, since the join killed the sockets")
        node_conns = {node: Connection(
            host=ip,
            user="azureadmin", # This dependency comes from terraform
            connect_kwargs={
                "key_filename": identity_file
            }
        ) for node, ip in nodes.items()}

        
        client_conns = {client: Connection(
            host=ip,
            user="azureadmin", # This dependency comes from terraform
            connect_kwargs={
                "key_filename": identity_file
            }
        ) for client, ip in clients.items()}

                
        print("Copying logs")
        copy_logs(node_conns, client_conns, i, curr_time)


@click.command()
@click.option(
    "-nt", "--node_template", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True),
    help="JSON template for node config"
)
@click.option(
    "-ct", "--client_template", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True),
    help="JSON template for client config"
)
@click.option(
    "-ips", "--ip_list", required=True,
    help="File with list of node names and IP addresses to be used with cluster config",
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-i", "--identity_file", required=True,
    help="SSH key",
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-r", "--repeat",
    default=1,
    help="Number of times to repeat experiment",
    type=click.INT
)
@click.option(
    "-s", "--seconds",
    default=30,
    help="Seconds to run each experiment",
    type=click.INT
)
def main(node_template, client_template, ip_list, identity_file, repeat, seconds):
    run_remote(node_template, client_template, ip_list, identity_file, repeat, seconds)

    
    
if __name__ == "__main__":
    main()