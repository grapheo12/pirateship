# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

from collections import OrderedDict
import os
from pprint import pprint
from typing import List, Tuple
import click
import json
from run_remote import build_project, get_current_git_hash, gen_config, tag_all_machines, CONFIG_SUFFIX, create_dirs_and_copy_files, tag_controller
from run_remote import run_nodes, run_clients, kill_clients, kill_nodes, copy_logs
from gen_cluster_config import gen_cluster_nodelist, DEFAULT_CA_NAME, LEARNER_SUFFIX, TLS_CERT_SUFFIX, TLS_PRIVKEY_SUFFIX, SIGN_PRIVKEY_SUFFIX, gen_keys_and_certs
import time
from fabric import Connection
import datetime
import glob
import matplotlib.pyplot as plt

from plot_utils import parse_log_dir_with_total_clients, plot_tput_vs_latency
from plot_throughput_timeseries import plot_throughput_timeseries_multi

def parse_reconfiguration_trace(trace_file, config_dir) -> List[Tuple[int, List[List[str]]]]:
    """
    Reconfiguration trace file format:
    `<second> <command> [name]`

    Example:
    ```
    1 ADD_LEARNER node5
    1 ADD_LEARNER node6
    2 UPGRADE_FULL_NODE node5
    5 DOWNGRADE_FULL_NODE node3
    10 DEL_LEARNER node3
    30 END
    ```
    """
    with open(trace_file, "r") as f:
        lines = f.readlines()
    commands = []
    for line in lines:
        parts = line.strip().split()
        cmd = []
        cmd.append(int(parts[0]))   # Second
        if not (parts[1] in ["ADD_LEARNER", "UPGRADE_FULL_NODE", "DOWNGRADE_FULL_NODE", "DEL_LEARNER", "END"]):
            raise Exception(f"Unknown command {parts[1]}")
        cmd.append(parts[1])
        if parts[1] == "ADD_LEARNER":
            # Find the appropriate one-liner learner info from configs
            if len(parts) != 3:
                raise Exception(f"ADD_LEARNER should have 1 operand")
            
            try:
                with open(f"{config_dir}/{parts[2]}{LEARNER_SUFFIX}", "r") as f:
                    learner_info = f.read().strip().split()
                parts.pop()
                parts.extend(learner_info)
            except Exception as e:
                pass

            if len(parts) != 6:
                raise Exception(f"ADD_LEARNER learner info not found for {parts[2]}")
        elif parts[1] != "END":
            if len(parts) != 3:
                raise Exception(f"{parts[1]} should have 1 operand")
        
        cmd.extend(parts[2:])
        commands.append(cmd)

    # Sort by time, group together commands at the same time
    times = {x[0] for x in commands}
    commands = [(t, [x[1:] for x in commands if x[0] == t]) for t in times]
    commands.sort(key=lambda x: x[0])

    return commands


def run_controller(conn: Connection, wd: str, cmd: str, repeat_num: int, log_num: int):
    prom = conn.run(f"cd pft/{wd} && ./target/release/controller configs/controller{CONFIG_SUFFIX} {cmd} > logs/{repeat_num}/controller_cmd{log_num}.log 2> logs/{repeat_num}/controller_cmd{log_num}.err",
                pty=True, hide=True, asynchronous=True)
    return prom


def get_cmd_str(cmd_list: List[List[str]]) -> str:
    ret = []
    for cmd in cmd_list:
        if cmd[0] == "ADD_LEARNER":
            ret.append(f"--add_learner '{cmd[1]} {cmd[2]} {cmd[3]} {cmd[4]}'")
        elif cmd[0] == "UPGRADE_FULL_NODE":
            ret.append(f"--upgrade_fullnode '{cmd[1]}'")
        elif cmd[0] == "DOWNGRADE_FULL_NODE":
            ret.append(f"--downgrade_fullnode '{cmd[1]}'")
        elif cmd[0] == "DEL_LEARNER":
            ret.append(f"--del_learner '{cmd[1]}'")

    return " ".join(ret)


def gen_extra_node_configs(outdir, extra_ip_list, template, num_init_config_nodes):
    # Need to contain the original config.
    # But only with the details specific to this node changed.
    cwd = os.getcwd()
    os.chdir(outdir)
    with open(template, "r") as f:
        cfg = json.load(f)
    
    nodelist, _ = gen_cluster_nodelist(extra_ip_list, ".pft.org", num_init_config_nodes)
    # Add the port numbers to the addresses
    nodelist = OrderedDict({
        k: (v[0] + ":" + str(num_init_config_nodes + 3001 + i), v[1])
        for i, (k, v) in enumerate(nodelist.items())
    })
    
    print("Number of extra nodes:", len(nodelist))

    gen_keys_and_certs(nodelist, DEFAULT_CA_NAME, 0, False, False)
    
    for i, node in enumerate(nodelist.keys()):
        cfg["net_config"]["name"] = node
        cfg["net_config"]["addr"] = "0.0.0.0:" + str(num_init_config_nodes + 3001 + i)
        cfg["net_config"]["tls_cert_path"] = f"{outdir}/{node}{TLS_CERT_SUFFIX}"
        cfg["net_config"]["tls_key_path"] = f"{outdir}/{node}{TLS_PRIVKEY_SUFFIX}"
        cfg["rpc_config"]["signing_priv_key_path"] = f"{outdir}/{node}{SIGN_PRIVKEY_SUFFIX}"

        # Change storage path to be distinct.
        # This will help colocate multiple nodes onto the same machine.
        if "log_storage_config" in cfg["consensus_config"]:
            if "RocksDB" in cfg["consensus_config"]["log_storage_config"]:
                cfg["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = f"/tmp/{node}_db"


        with open(f"{node}{CONFIG_SUFFIX}", "w") as f:
            json.dump(cfg, f)



    os.chdir(cwd)



    
def run_with_given_reconfiguration_trace(node_template, client_template, ip_list, extra_ip_list, identity_file, trace_file, repeat, git_hash, client_n):
    # This is almost same as run_remote.
    # But we will modify each clients config after all the configs are generated.
    # Then we will copy them over to remote and run experiments.
    gen_config("configs", "cluster", node_template, client_template, ip_list, -1)


    nodes, clients = tag_all_machines(ip_list)
    extra_nodes, _ = tag_all_machines(extra_ip_list, len(nodes))

    # Generate configs for extra nodes
    gen_extra_node_configs("configs", extra_ip_list, f"{list(nodes.keys())[0]}{CONFIG_SUFFIX}", len(nodes))
    controller_ip = tag_controller(ip_list)

    # Parse the trace file
    try:
        commands = parse_reconfiguration_trace(trace_file, "configs")
    except Exception as e:
        print(e)
        return
    # How many client goes in each machine?
    num_clients = [client_n // len(clients) for _ in range(len(clients))]
    num_clients[-1] += client_n % len(clients)

    for i, client in enumerate(clients.keys()):
        with open(f"configs/{client}{CONFIG_SUFFIX}", "r") as f:
            cfg = json.load(f)

        cfg["workload_config"]["num_clients"] = num_clients[i]

        with open(f"configs/{client}{CONFIG_SUFFIX}", "w") as f:
            json.dump(cfg, f)

    # The following is EXACTLY same as run_remote
    print("Creating SSH connections")
    node_conns = {node: Connection(
        host=ip,
        user="pftadmin", # This dependency comes from terraform
        connect_kwargs={
            "key_filename": identity_file
        }
    ) for node, ip in nodes.items()}

    
    client_conns = {client: Connection(
        host=ip,
        user="pftadmin", # This dependency comes from terraform
        connect_kwargs={
            "key_filename": identity_file
        }
    ) for client, ip in clients.items()}

    controller_conn = Connection(
        host=controller_ip,
        user="pftadmin", # This dependency comes from terraform
        connect_kwargs={
            "key_filename": identity_file
        }
    )

    curr_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    print("Current working directory", curr_time)

    print("Copying files")
    create_dirs_and_copy_files(node_conns, client_conns, curr_time, repeat, git_hash, controller_conn)

    for i in range(repeat):
        print("Experiment sequence num:", i)

        print("Running Nodes")
        promises = []
        promises.extend(run_nodes(node_conns, i, curr_time))

        time.sleep(1)

        print("Running clients")
        promises.extend(run_clients(client_conns, i, curr_time))

        extra_node_conns = {}
        extra_promises = []

        for j, cmd in enumerate(commands):
            for c in cmd[1]:
                if c[0] == "ADD_LEARNER":
                    # Boot up this node
                    print("Booting up extra node:", c[1])
                    extra_node_conns[c[1]] = Connection(
                        host=extra_nodes[c[1]],
                        user="pftadmin", # This dependency comes from terraform
                        connect_kwargs={
                            "key_filename": identity_file
                        }
                    )
                    create_dirs_and_copy_files({c[1]: extra_node_conns[c[1]]}, {}, curr_time, repeat, git_hash, None)
                    time.sleep(0.1)
                    extra_promises.append(run_nodes({c[1]: extra_node_conns[c[1]]}, i, curr_time))

            if j == 0:
                sleep_time = cmd[0]
            else:
                sleep_time = cmd[0] - commands[j - 1][0]
            print("Running reconfiguration command", j, ": Sleeping for", sleep_time, "seconds")
            time.sleep(sleep_time)
            print(f"Running reconfiguration command {j}: {cmd[1]}")
            if len(cmd[1]) == 0 or len(cmd[1][0]) == 0 or cmd[1][0][0] == "END":
                print("Ending")
                break


            prom = run_controller(controller_conn, curr_time, get_cmd_str(cmd[1]), i, j)
            try:
                prom.join()
            except Exception as e:
                print(e)

            for c in cmd[1]:
                if c[0] == "DEL_LEARNER":
                    time.sleep(0.1)
                    if c[1] in extra_node_conns:
                        print("Killing extra node:", c[1])
                        kill_nodes({c[1]: extra_node_conns[c[1]]})
                    elif c[1] in node_conns:
                        print("Killing node:", c[1])
                        kill_nodes({c[1]: node_conns[c[1]]})
                    
        
        time.sleep(1)
        
        print("Killing clients")
        kill_clients(client_conns)

        print("Killing nodes")
        kill_nodes(node_conns)

        print("Killing extra nodes")
        kill_nodes(extra_node_conns)

        print("Joining on all promises (which should have terminated by now)")
        for prom in promises:
            try:
                prom.join()
            except Exception as e:
                print(e)

        print("Reestablishing connections, since the join killed the sockets")
        node_conns = {node: Connection(
            host=ip,
            user="pftadmin", # This dependency comes from terraform
            connect_kwargs={
                "key_filename": identity_file
            }
        ) for node, ip in nodes.items()}

        extra_node_conns = {node: Connection(
            host=ip,
            user="pftadmin", # This dependency comes from terraform
            connect_kwargs={
                "key_filename": identity_file
            }
        ) for node, ip in extra_nodes.items() if node in extra_node_conns}

        
        client_conns = {client: Connection(
            host=ip,
            user="pftadmin", # This dependency comes from terraform
            connect_kwargs={
                "key_filename": identity_file
            }
        ) for client, ip in clients.items()}

        controller_conn = Connection(
            host=controller_ip,
            user="pftadmin", # This dependency comes from terraform
            connect_kwargs={
                "key_filename": identity_file
            }
        )

        print("Copying logs")
        copy_logs(node_conns, client_conns, i, curr_time, controller_conn, len(commands) - 1) # End is not counted
        copy_logs(extra_node_conns, {}, i, curr_time)

    # Copy the number of clients in log directory for safekeeping
    with open(f"logs/{curr_time}/num_clients.txt", "w") as f:
        print(client_n, file=f)

    return curr_time




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
    "-tr", "--trace_file", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True),
    help="Trace file for reconfiguration commands"
)
@click.option(
    "-ips", "--ip_list", required=True,
    help="File with list of node names and IP addresses to be used with cluster config",
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-eips", "--extra_ip_list", required=True,
    help="File with list of node names and IP addresses to be used in later configurations",
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
    "-c", "--client", required=True,
    help="Number of clients",
    type=click.INT
)
@click.option(
    "-t", "--target", multiple=True,
    help="Nodes to plot throughput",
    type=click.STRING
)
def main(node_template, client_template, trace_file, ip_list, extra_ip_list, identity_file, repeat, client, target):
    # build_project()
    git_hash = get_current_git_hash()
    _, client_tags = tag_all_machines(ip_list)
    client_machines = len(client_tags)

    dir = run_with_given_reconfiguration_trace(
            node_template, client_template, ip_list, extra_ip_list,
            identity_file, trace_file, repeat, git_hash,
            client)
    dir = f"logs/{dir}"

    for i in range(repeat):
        if len(target) == 0:
            infiles = glob.glob(f"{dir}/{i}/node*.log")
            legends = [os.path.basename(a).split(".")[0] for a in infiles]
        else:
            infiles = [f"{dir}/{i}/{t}.log" for t in target]
            legends = target[:]

        outfile = f"{dir}/{i}/plot.png"

        cmd_files = glob.glob(f"{dir}/{i}/controller_cmd*.log")

        plot_throughput_timeseries_multi(infiles, legends, outfile, 0, 0, cmd_files)
        plt.clf()


if __name__ == "__main__":
    main()