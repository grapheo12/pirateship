from pprint import pprint
import click
import json
from run_remote import build_project, get_current_git_hash, gen_config, tag_all_machines, CONFIG_SUFFIX, create_dirs_and_copy_files
from run_remote import run_nodes, run_clients, kill_clients, kill_nodes, copy_logs
import time
from fabric import Connection
import datetime

from plot_utils import parse_log_dir_with_total_clients, plot_tput_vs_latency


def run_with_given_client(node_template, client_template, ip_list, identity_file, repeat, seconds, git_hash, client_n, max_nodes=-1):
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
    "-c", "--clients", multiple=True, required=True,
    help="List of number of clients to sweep over",
    type=click.INT
)
@click.option(
    "-up", "--ramp_up",
    default=2,
    help="Ramp up seconds to ignore in plotting",
    type=click.INT
)
@click.option(
    "-down", "--ramp_down",
    default=2,
    help="Ramp down seconds to ignore in plotting",
    type=click.INT
)
@click.option(
    "-nodes", "--max_nodes",
    default=-1,
    help="Maximum number of nodes to use",
    type=click.INT
)
def main(node_template, client_template, ip_list, identity_file, repeat, seconds, clients, ramp_up, ramp_down, max_nodes):
    # build_project()
    git_hash = get_current_git_hash()
    _, client_tags = tag_all_machines(ip_list)
    client_machines = len(client_tags)

    dirs = []
    for client in list(clients):
        dirs.append(
            run_with_given_client(
                node_template, client_template, ip_list,
                identity_file, repeat, seconds, git_hash,
                client, max_nodes)
        )
    dirs = [f"logs/{d}" for d in dirs]

    stats = {}
    for d in dirs:
        res = parse_log_dir_with_total_clients(
            d, repeat, client_machines, "node1",
            ramp_up, ramp_down)
        stats.update(res)

    pprint(stats)
    plot_tput_vs_latency(stats, f"{dirs[-1]}/plot.png")

    


if __name__ == "__main__":
    main()