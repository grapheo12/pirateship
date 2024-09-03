# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

import click
from run_remote import run_all, tag_all_machines
from fabric import Connection

@click.command()
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
def main(ip_list, identity_file):
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

    print("Closing orphan processes")
    for conn in node_conns.values():
        print(run_all([
            "pkill -c server"
        ], conn))
    
    for conn in client_conns.values():
        print(run_all([
            "pkill -c client"
        ], conn))


    print("Deleting everything in $HOME/pft")
    for conn in node_conns.values():
        run_all([
            "rm -rf pft"
        ], conn)

    for conn in client_conns.values():
        run_all([
            "rm -rf pft"
        ], conn)


if __name__ == "__main__":
    main()

