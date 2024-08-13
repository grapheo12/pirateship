from pprint import pprint
from typing import List, Tuple
import click
import json
from run_remote import build_project, get_current_git_hash, gen_config, tag_all_machines, CONFIG_SUFFIX, create_dirs_and_copy_files, tag_controller
from run_remote import run_nodes, run_clients, kill_clients, kill_nodes, copy_logs
import time
from fabric import Connection
import datetime

from plot_utils import parse_log_dir_with_total_clients, plot_tput_vs_latency

def parse_reconfiguration_trace(trace_file) -> List[Tuple[int, List[List[str]]]]:
    """
    Reconfiguration trace file format:
    `<second> <command> [operands]`

    Example:
    ```
    1 ADD_LEARNER node5 10.2.0.7:3005 node5.pft.org MCowgjksngjkngfsgn...(Public key PEM)
    1 ADD_LEARNER node6 10.2.0.7:3006 node6.pft.org MCowgjksngjkngfsgn...(Public key PEM)
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
            if len(parts) != 6:
                raise Exception(f"ADD_LEARNER should have 4 operands")
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

    
def run_with_given_reconfiguration_trace(node_template, client_template, ip_list, identity_file, trace_file, repeat, git_hash, client_n):
    # Parse the trace file
    try:
        commands = parse_reconfiguration_trace(trace_file)
    except Exception as e:
        print(e)
        return
    
    # This is almost same as run_remote.
    # But we will modify each clients config after all the configs are generated.
    # Then we will copy them over to remote and run experiments.
    gen_config("configs", "cluster", node_template, client_template, ip_list, -1)
    nodes, clients = tag_all_machines(ip_list)
    controller_ip = tag_controller(ip_list)

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

    controller_conn = Connection(
        host=controller_ip,
        user="azureadmin", # This dependency comes from terraform
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

        for j, cmd in enumerate(commands):
            print("Running reconfiguration command", j, ": Sleeping for", cmd[0], "seconds")
            time.sleep(cmd[0])
            print(f"Running reconfiguration command {j}: {cmd[1]}")
            if len(cmd[1]) == 0 or cmd[1][0] == "END":
                break

            prom = run_controller(controller_conn, curr_time, get_cmd_str(cmd[1]), i, j)
            try:
                prom.join()
            except Exception as e:
                print(e)
        
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

        controller_conn = Connection(
            host=controller_ip,
            user="azureadmin", # This dependency comes from terraform
            connect_kwargs={
                "key_filename": identity_file
            }
        )

        print("Copying logs")
        copy_logs(node_conns, client_conns, i, curr_time, controller_conn, len(commands))

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
def main(node_template, client_template, trace_file, ip_list, identity_file, repeat, client):
    # build_project()
    git_hash = get_current_git_hash()
    _, client_tags = tag_all_machines(ip_list)
    client_machines = len(client_tags)

    dir = run_with_given_reconfiguration_trace(
            node_template, client_template, ip_list,
            identity_file, trace_file, repeat, git_hash,
            client)
    dir = f"logs/{dir}"

    # stats = {}
    # for d in dirs:
    #     res = parse_log_dir_with_total_clients(
    #         d, repeat, client_machines, "node1",
    #         ramp_up, ramp_down)
    #     stats.update(res)

    # pprint(stats)
    # plot_tput_vs_latency(stats, f"{dirs[-1]}/plot.png")

    


if __name__ == "__main__":
    main()