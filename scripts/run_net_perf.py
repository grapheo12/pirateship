# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

from pprint import pprint
import click
import json
from run_remote import build_project, get_current_git_hash, gen_config, tag_all_machines, CONFIG_SUFFIX, create_dirs_and_copy_files
from run_remote import run_nodes_with_net_perf, kill_nodes_with_net_perf, copy_logs
import time
from fabric import Connection
import datetime

from plot_utils import parse_log_dir_with_total_clients, plot_tput_vs_latency


def run_net_perf(node_template, client_template, ip_list, identity_file, repeat, seconds, git_hash, client_n, max_nodes=-1, payload_sz=4096):
    # This is almost same as run_remote.
    # But we will modify each clients config after all the configs are generated.
    # Then we will copy them over to remote and run experiments.
    gen_config("configs", "cluster", node_template, client_template, ip_list, max_nodes)
    nodes, clients = tag_all_machines(ip_list, start=0, max_nodes=max_nodes)

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

    
    # client_conns = {client: Connection(
    #     host=ip,
    #     user="pftadmin", # This dependency comes from terraform
    #     connect_kwargs={
    #         "key_filename": identity_file
    #     }
    # ) for client, ip in clients.items()}
    client_conns = {}
    curr_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    print("Current working directory", curr_time)

    print("Copying files")
    create_dirs_and_copy_files(node_conns, client_conns, curr_time, repeat, git_hash)

    for i in range(repeat):
        print("Experiment sequence num:", i)

        print("Running Nodes")
        promises = []
        promises.extend(run_nodes_with_net_perf(node_conns, i, curr_time, payload_sz))

        time.sleep(1)

        # print("Running clients")
        # promises.extend(run_clients(client_conns, i, curr_time))

        print("Running experiments for", seconds, "seconds")
        time.sleep(seconds)
        
        # print("Killing clients")
        # kill_clients(client_conns)

        print("Killing nodes")
        kill_nodes_with_net_perf(node_conns)

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

        
        # client_conns = {client: Connection(
        #     host=ip,
        #     user="pftadmin", # This dependency comes from terraform
        #     connect_kwargs={
        #         "key_filename": identity_file
        #     }
        # ) for client, ip in clients.items()}

                
        print("Copying logs")
        copy_logs(node_conns, client_conns, i, curr_time)

    # # Copy the number of clients in log directory for safekeeping
    # with open(f"logs/{curr_time}/num_clients.txt", "w") as f:
    #     print(client_n, file=f)

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
    default=60,
    help="Seconds to run each experiment",
    type=click.INT
)
@click.option(
    "-nodes", "--max_nodes",
    default=-1,
    help="Maximum number of nodes to use",
    type=click.INT
)
@click.option(
    "-sz", "--size",
    default=4096,
    help="Payload size",
    type=click.INT
)
def main(node_template, client_template, ip_list, identity_file, repeat, seconds, max_nodes, size):
    # build_project()
    git_hash = get_current_git_hash()
    tag_all_machines(ip_list)

    clients = [1]
    dirs = []
    for client in list(clients):
        dirs.append(
            run_net_perf(
                node_template, client_template, ip_list,
                identity_file, repeat, seconds, git_hash,
                client, max_nodes, size)
        )

    


if __name__ == "__main__":
    main()