import tomli
import click
from pprint import pprint
import os.path
from dataclasses import dataclass
from copy import deepcopy
from itertools import product

"""
Each experiment goes through 7 phases:

1. Deploying VMs
2. Copying the working tree to one of the VMs (`coordinator`).
3. Building the code.
4. Actually running the experiment which dumps logs in the respective VMs.
5. Collecting logs in coordinator.
6. Copying the logs back to local.
7. Destroying the VMs.

Step 1 and 7 are usually handled by Terraform. However, they can also be injected manually.
For Step 2, we choose the 1st client VM.
The experiments are usually of the form:

{ set of clients } <----------> { set of nodes }
Hence describing an experiment only needs 3 things: Workload used by clients, Protocol used by nodes, App running on the nodes.
The collected logs can then be used to plot different graphs.

The only exceptions are reconfiguration experiments, which requires an additional coordinator client.
See config.toml for a sample TOML config format.
```
"""

@dataclass
class Node:
    name: str
    public_ip: str
    private_ip: str
    tee_type: str
    region_id: int
    is_client: bool
    is_coordinator: bool


@dataclass
class Experiment:
    name: str
    repeats: int
    duration: int
    num_nodes: int
    num_clients: int
    base_node_config: dict
    base_client_config: dict
    node_distribution: str
    build_command: str



def nested_override(source, diff):
    target = deepcopy(source)
    for k, v in diff.items():
        if not(k in target): # New key
            target[k] = deepcopy(v)
        elif not(isinstance(target[k], dict)):  # target[k] is not nested
            target[k] = deepcopy(v)
        elif not(isinstance(v, dict)):      # v is not nested: Hitting this condition means the dict is malformed (doesn't conform to a schema)
            target[k] = deepcopy(v)
        else: # Nested object
            new_v = nested_override(target[k], v)
            target[k] = new_v

    return target


# Can't handle nested list (ie, list of lists)
# Nested dicts are ok
def flatten_sweeping_params(e):
    work_items = []
    for k, v in e.items():
        if isinstance(v, dict):
            v_cross_product = flatten_sweeping_params(v)
            for _e in v_cross_product:
                work_items.append(nested_override(e, {k: _e}))

    if len(work_items) == 0:
        work_items = [e]

    ret = []
    for _e in work_items:
        list_items = []
        for k, v in _e.items():
            if isinstance(v, list):
                list_items.append(
                    [(k, it) for it in v]
                )
        prod = list(product(*list_items))
        for p in prod:
            print(p)
            override = {}
            for (k, v) in p:
                override[k] = v
            
            ret.append(nested_override(_e, override))

    return ret



class Deployment:
    def __init__(self, config, workdir):
        self.mode = config["mode"]
        self.nodelist = []
        self.workdir = workdir
        first_client = False
        if self.mode == "manual":
            for name, info in config["node_list"].items():
                public_ip = info["public_ip"]
                private_ip = info["private_ip"]
                is_client = name.startswith("client")
                if is_client and not(first_client):
                    is_coordinator = True
                    first_client = True
                else:
                    is_coordinator = False
                if not(is_client):
                    tee_type = info["tee_type"]
                else:
                    tee_type = "nontee"
                
                if "region_id" in info:
                    region_id = int(info["region_id"])
                else:
                    region_id = 0

                self.nodelist.append(Node(name, public_ip, private_ip, tee_type, region_id, is_client, is_coordinator))
        
        if not(first_client):
            raise ValueError("No client VM")

        self.ssh_user = config["ssh_user"]
        self.ssh_key = os.path.join(workdir, config["ssh_key"])
        self.node_port_base = int(config["node_port_base"])

    def deploy(self):
        if self.mode == "manual":
            return
        
        # TODO: Terraform deploy
        
    def teardown(self):
        if self.mode == "manual":
            return
        
        # TODO: Terraform teardown

    def __repr__(self):
        s = f"Mode: {self.mode}\n"
        s += f"SSH user: {self.ssh_user}\n"
        s += f"SSH key path: {self.ssh_key}\n"
        s += f"Node port base: {self.node_port_base}\n"
        s += "++++ Nodelist +++++\n"
        for node in self.nodelist:
            s += f"{node}\n"

        return s



def parse_config(path):
    with open(path, "rb") as f:
        toml_dict = tomli.load(f)

    pprint(toml_dict)
    workdir = toml_dict["workdir"]
    deployment = Deployment(toml_dict["deployment_config"], workdir)

    base_node_config = toml_dict["node_config"]
    base_client_config = toml_dict["client_config"]

    experiments = []
    for e in toml_dict["experiments"]:
        node_config = nested_override(base_node_config, e.get("node_config", {}))
        client_config = nested_override(base_client_config, e.get("client_config", {}))

        if "sweeping_parameters" in e:
            flats = flatten_sweeping_params(e["sweeping_parameters"])
            for i, params in enumerate(flats):
                _e = nested_override(e, params)
                experiments.append(Experiment(
                    f"{_e['name']}/{i}",
                    int(_e["repeats"]),
                    int(_e["duration"]),
                    int(_e["num_nodes"]),
                    int(_e["num_clients"]),
                    node_config,
                    client_config,
                    _e.get("node_distribution", "uniform"),
                    _e.get("build_command", "make")
                ))
        else:
            experiments.append(Experiment(
                e["name"],
                int(e["repeats"]),
                int(e["duration"]),
                int(e["num_nodes"]),
                int(e["num_clients"]),
                node_config,
                client_config,
                e.get("node_distribution", "uniform"),
                e.get("build_command", "make")
            ))

    print(deployment)
    pprint(experiments)


@click.command()
@click.option(
    "-c", "--config", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
def main(config):
    parse_config(config)


if __name__ == "__main__":
    main()


