import tomli
import click
from click_default_group import DefaultGroup
import invoke
from fabric import Connection
from fabric.runners import Result
from pprint import pprint
import os.path
from dataclasses import dataclass
from copy import deepcopy
from itertools import product
from collections.abc import Callable
import datetime
import json
from crypto import gen_keys_and_certs, TLS_CERT_SUFFIX, TLS_PRIVKEY_SUFFIX, ROOT_CERT_SUFFIX, PUB_KEYLIST_NAME, SIGN_PRIVKEY_SUFFIX
import pickle
import re


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

def run_local(cmds: list, hide=True):
    results = []
    for cmd in cmds:
        res = invoke.run(cmd, hide=hide)
        results.append(res.stdout.strip())

    return results


@dataclass
class Node:
    name: str
    public_ip: str
    private_ip: str
    tee_type: str
    region_id: int
    is_client: bool
    is_coordinator: bool


def run_remote_public_ip(cmds: list, ssh_user, ssh_key, host: Node, hide=True):
    results = []
    conn = Connection(
        host=host.public_ip,
        user=ssh_user,
        connect_kwargs={
            "key_filename": ssh_key
        }
    )
    for cmd in cmds:
        res = conn.run(cmd, hide=hide, pty=True)
        results.append(res.stdout.strip())

    return results

def copy_remote_public_ip(src, dest, ssh_user, ssh_key, host: Node):
    conn = Connection(
        host=host.public_ip,
        user=ssh_user,
        connect_kwargs={
            "key_filename": ssh_key
        }
    )

    conn.put(src, remote=dest)


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
        self.raw_config = deepcopy(config)
        first_client = False
        if self.mode == "manual":
            self.populate_nodelist()

        self.ssh_user = config["ssh_user"]

        if os.path.isabs(config["ssh_key"]):
            self.ssh_key = config["ssh_key"]
        else:
            self.ssh_key = os.path.join(workdir, "deployment", config["ssh_key"])
        self.node_port_base = int(config["node_port_base"])

    def find_azure_tf_dir(self):
        search_paths = [
            os.path.join("deployment", "azure-tf"),
            os.path.join("scripts_v2", "deployment", "azure-tf"),
        ]

        found_path = None
        for path in search_paths:
            if os.path.exists(path):
                found_path = os.path.abspath(path)
                break

        if found_path is None:
            raise FileNotFoundError("Azure Terraform directory not found")
        else:
            print(f"Found Azure Terraform directory at {found_path}")

        return found_path
    
    def prepare_dev_vm(self):
        if self.dev_vm is None:
            raise ValueError("No dev VM found")
        
        # Find __prepare_dev_env.sh and ideal_bashrc
        search_paths = [
            "deployment",
            os.path.join("scripts_v2", "deployment"),
            os.path.join("deployment", "azure-tf"),
            os.path.join("scripts_v2", "deployment", "azure-tf"),
        ]

        for path in search_paths:
            if os.path.exists(os.path.join(path, "__prepare-dev-env.sh")) and os.path.exists(os.path.join(path, "ideal_bashrc")):
                found_path = os.path.abspath(path)
                break
        else:
            raise FileNotFoundError("Dev VM setup scripts not found")
        
        # Copy the scripts to the dev VM
        copy_remote_public_ip(os.path.join(found_path, "__prepare-dev-env.sh"), f"/home/{self.ssh_user}/__prepare-dev-env.sh", self.ssh_user, self.ssh_key, self.dev_vm)
        copy_remote_public_ip(os.path.join(found_path, "ideal_bashrc"), f"/home/{self.ssh_user}/ideal_bashrc", self.ssh_user, self.ssh_key, self.dev_vm)

        # Run the scripts
        run_remote_public_ip([
            "chmod +x __prepare-dev-env.sh",
            "./__prepare-dev-env.sh"
        ], self.ssh_user, self.ssh_key, self.dev_vm, hide=False)

        
    def get_ssh_key(self):
        tf_output_dir = os.path.abspath(os.path.join(self.workdir, "deployment"))
        tfstate_path = os.path.join(tf_output_dir, "terraform.tfstate")
        private_key = run_local([
            f"terraform output -state={tfstate_path} --raw cluster_private_key"
        ])[0]
        with open(self.ssh_key, "w") as f:
            f.write(private_key)

        run_local([
            "chmod 600 " + self.ssh_key
        ])


    def deploy(self):
        run_local([
            f"mkdir -p {self.workdir}",
            f"mkdir -p {self.workdir}/deployment",
        ])
        with open(os.path.join(self.workdir, "deployment", "deployment.txt"), "w") as f:
            pprint(self, f)

        if self.mode == "manual":
            return
        
        # Terraform deploy

        # Find the azure-tf directory relative to where the script is being called from
        found_path = self.find_azure_tf_dir()

        if found_path is None:
            raise FileNotFoundError("Azure Terraform directory not found")
        else:
            print(f"Found Azure Terraform directory at {found_path}")

        # There must exist a var-file in azure-tf/setups for the deployment mode
        var_file = os.path.join(found_path, "setups", f"{self.mode}.tfvars")
        if not os.path.exists(var_file):
            raise FileNotFoundError(f"Var file for deployment mode {self.mode} not found")

        tf_output_dir = os.path.abspath(os.path.join(self.workdir, "deployment"))
        plan_path = os.path.join(tf_output_dir, "main.tfplan")
        tfstate_path = os.path.join(tf_output_dir, "terraform.tfstate")

        # Plan
        run_local([
            f"terraform -chdir={found_path} init",
            f"terraform -chdir={found_path} plan -no-color -var-file=\"{var_file}\" -var=\"username={self.ssh_user}\" -out={plan_path} -state={tfstate_path} > {tf_output_dir}/plan.log 2>&1",
        ])

        # Apply
        run_local([
            f"terraform -chdir={found_path} apply -no-color -auto-approve -state={tfstate_path} {plan_path} > {tf_output_dir}/apply.log 2>&1",
        ])

        # Populate nodelist
        self.populate_nodelist()

        # Store the SSH key
        self.get_ssh_key()

        # Install dev dependencies on dev VM
        self.prepare_dev_vm()

        # Save myself
        with open(os.path.join(self.workdir, "deployment", "deployment.pkl"), "wb") as f:
            pickle.dump(self, f)

        # Rewrite deployment.txt
        with open(os.path.join(self.workdir, "deployment", "deployment.txt"), "w") as f:
            pprint(self, f)


    def populate_raw_node_list_from_terraform(self):
        found_path = self.find_azure_tf_dir()

        if found_path is None:
            raise FileNotFoundError("Azure Terraform directory not found")
        
        tf_output_dir = os.path.abspath(os.path.join(self.workdir, "deployment"))
        tfstate_path = os.path.join(tf_output_dir, "terraform.tfstate")

        rg_name = run_local([
            f"terraform output -state={tfstate_path} --raw resource_group_name"
        ])[0]

        print("Resource group name:", rg_name)

        # Use az cli to get the public and private IPs of all deployed vms in this resource group
        private_ips, public_ips = run_local([
            r'az vm list-ip-addresses --resource-group ' + rg_name
            + r' --query "[].{Name:virtualMachine.name, IP:virtualMachine.network.privateIpAddresses[0]}" --output json',

            r'az vm list-ip-addresses --resource-group ' + rg_name
            + r' --query "[].{Name:virtualMachine.name, IP:virtualMachine.network.publicIpAddresses[0].ipAddress}" --output json'
        ])

        private_ips = json.loads(private_ips)
        public_ips = json.loads(public_ips)

        vm_names = {vm["Name"] for vm in private_ips}

        node_list = {}
        for name in vm_names:
            private_ip = next(vm["IP"] for vm in private_ips if vm["Name"] == name)
            public_ip = next(vm["IP"] for vm in public_ips if vm["Name"] == name)
            
            if "tdx" in name:
                tee_type = "tdx"
            elif "sev" in name:
                tee_type = "sev"
            else:
                tee_type = "nontee"
            
            if "loc" in name:
                # Find the _locX_ part
                region_id = int(re.findall(r"loc(\d+)", name)[0])
            else:
                region_id = 0

            node_list[name] = {
                "private_ip": private_ip,
                "public_ip": public_ip,
                "tee_type": tee_type,
                "region_id": region_id
            }

        self.raw_config["node_list"] = node_list

        pprint(node_list)


    def populate_nodelist(self):
        if self.mode != "manual":
            self.populate_raw_node_list_from_terraform()
    
        first_client = False
        for name, info in self.raw_config["node_list"].items():
            public_ip = info["public_ip"]
            private_ip = info["private_ip"]
            is_client = name.startswith("client")
            dev_vm = False
            if is_client and not(first_client):
                is_coordinator = True
                first_client = True
                dev_vm = True
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

            if dev_vm:
                self.dev_vm = self.nodelist[-1]
        
        if not(first_client):
            raise ValueError("No client VM")

        
    def teardown(self):
        if self.mode == "manual":
            return
        
        found_path = self.find_azure_tf_dir()
        if found_path is None:
            raise FileNotFoundError("Azure Terraform directory not found")
        else:
            print(f"Found Azure Terraform directory at {found_path}")

        tf_output_dir = os.path.abspath(os.path.join(self.workdir, "deployment"))
        tfstate_path = os.path.join(tf_output_dir, "terraform.tfstate")


        while True:
            try:
                run_local([
                    f"terraform -chdir={found_path} apply -destroy -no-color -auto-approve -state={tfstate_path}",
                ], hide=False)
                break
            except Exception as e:
                print("Error while destroying VMs. Retrying...")
                print(e)

    def __repr__(self):
        s = f"Mode: {self.mode}\n"
        s += f"SSH user: {self.ssh_user}\n"
        s += f"SSH key path: {self.ssh_key}\n"
        s += f"Node port base: {self.node_port_base}\n"
        s += "++++ Nodelist +++++\n"
        for node in self.nodelist:
            s += f"{node}\n"

        return s
    
    def get_all_node_vms(self):
        return [
            vm for vm in self.nodelist if "node" in vm.name
        ]
    
    def get_all_client_vms(self):
        return [
            vm for vm in self.nodelist if "client" in vm.name
        ]
    
    def get_nodes_with_tee(self, tee):
        return [
            vm for vm in self.get_all_node_vms() if tee in vm.tee_type
        ]


DEFAULT_CA_NAME = "Pft"

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

    def create_directory(self, workdir):
        build_dir = os.path.join(workdir, "build")
        config_dir = os.path.join(workdir, "configs")
        log_dirs = [
            os.path.join(workdir, "logs", str(i))
            for i in range(self.repeats)
        ]

        run_local([
            f"mkdir -p {d}"
            for d in [build_dir] + [config_dir] + log_dirs
        ])

        return build_dir, config_dir, log_dirs

    def tag_source(self, workdir):
        git_hash, patch = run_local([
            "git rev-parse HEAD",
            "git diff"
        ])

        with open(os.path.join(workdir, "git_hash.txt"), "w") as f:
            f.write(git_hash)

        with open(os.path.join(workdir, "diff.patch"), "w") as f:
            f.write(patch)


    def __gen_crypto(self, config_dir, nodelist, client_cnt):
        participants = gen_keys_and_certs(nodelist, DEFAULT_CA_NAME, client_cnt, config_dir)
        print(participants)
        return {
            k: (
                os.path.join(config_dir, f"{k}{TLS_CERT_SUFFIX}"), # tls_cert_path
                os.path.join(config_dir, f"{k}{TLS_PRIVKEY_SUFFIX}"), # tls_key_path
                os.path.join(config_dir, f"{DEFAULT_CA_NAME}{ROOT_CERT_SUFFIX}"), # tls_root_ca_cert_path
                os.path.join(config_dir, PUB_KEYLIST_NAME), # allowed_keylist_path
                os.path.join(config_dir, f"{k}{SIGN_PRIVKEY_SUFFIX}"), # signing_priv_key_path
            ) for k in participants
        }
        

    def generate_configs(self, deployment: Deployment, config_dir):
        # Number of nodes in deployment may be < number of nodes in deployment
        # So we reuse nodes.
        # As a default, each deployed node gets its unique port number
        # So there will be no port clash.
        rr_cnt = 0
        nodelist = []
        nodes = {}
        node_configs = {}
        vms = []
        node_list_for_crypto = {}
        if self.node_distribution == "uniform":
            vms = deployment.get_all_node_vms()
        else:
            vms = deployment.get_nodes_with_tee(self.node_distribution)

        for node_num in range(1, self.num_nodes+1):
            port = deployment.node_port_base + node_num
            listen_addr = f"0.0.0.0:{port}"
            name = f"node{node_num}"
            domain = f"{name}.pft.org"
            
            private_ip = vms[rr_cnt % len(vms)].private_ip
            rr_cnt += 1
            connect_addr = f"{private_ip}:{port}"

            nodelist.append(name[:])
            nodes[name] = {
                "addr": connect_addr,
                "domain": domain
            }

            node_list_for_crypto[name] = (connect_addr, domain)

            config = deepcopy(self.base_node_config)
            config["net_config"]["name"] = name
            config["net_config"]["addr"] = listen_addr
            config["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = f"./{name}-db"


            node_configs[name] = config

        client_vms = deployment.get_all_client_vms()
        crypto_info = self.__gen_crypto(config_dir, node_list_for_crypto, len(client_vms))
        

        for k, v in node_configs.items():
            tls_cert_path, tls_key_path, tls_root_ca_cert_path,\
            allowed_keylist_path, signing_priv_key_path = crypto_info[k]

            v["net_config"]["nodes"] = deepcopy(nodes)
            v["consensus_config"]["node_list"] = nodelist[:]
            v["consensus_config"]["learner_list"] = []
            v["net_config"]["tls_cert_path"] = tls_cert_path
            v["net_config"]["tls_key_path"] = tls_key_path
            v["net_config"]["tls_root_ca_cert_path"] = tls_root_ca_cert_path
            v["rpc_config"]["allowed_keylist_path"] = allowed_keylist_path
            v["rpc_config"]["signing_priv_key_path"] = signing_priv_key_path

            with open(os.path.join(config_dir, f"{k}_config.json"), "w") as f:
                json.dump(v, f, indent=4)


    def tag_experiment(self, workdir):
        with open(os.path.join(workdir, "experiment.txt"), "w") as f:
            pprint(self, f)


    def run(self, deployment: Deployment):
        # Create the necessary directories
        workdir = os.path.join(deployment.workdir, self.name)
        build_dir, config_dir, log_dirs = self.create_directory(workdir)
        self.tag_source(workdir)
        self.tag_experiment(workdir)
        self.generate_configs(deployment, config_dir)
        


def print_dummy(experiments, **kwargs):
    pprint(experiments)
    print(kwargs)

@dataclass
class Result:
    plotter_func: Callable
    experiments: list
    kwargs: dict

    def __init__(self, fn_name, experiments, kwargs):
        self.plotter_func = print_dummy # TODO: Port plotting functions
        self.experiments = experiments[:]
        self.kwargs = deepcopy(kwargs)

    def output(self):
        self.plotter_func(self.experiments, **self.kwargs)



def parse_config(path, workdir=None):
    with open(path, "rb") as f:
        toml_dict = tomli.load(f)

    pprint(toml_dict)

    if workdir is None:
        curr_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        workdir = os.path.join(toml_dict["workdir"], curr_time)

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
                    os.path.join(_e['name'], str(i)),
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

    results = []
    for r in toml_dict["results"]:
        args = deepcopy(r)
        del args["plotter"]
        results.append(
            Result(r["plotter"], experiments, args)
        )

    return (deployment, experiments, results)


@click.group(cls=DefaultGroup, default='all', default_if_no_args=True, help="Run experiment pipeline (runs all if no subcommand is provided)")
@click.pass_context
def main(ctx):
    if ctx.invoked_subcommand is None:
        # Run all()
        ctx.invoke(all)

@main.command()
@click.option(
    "-c", "--config", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-d", "--workdir", required=False,
    type=click.Path(file_okay=False, resolve_path=True),
    default=None
)
def all(config, workdir):
    deployment, experiments, results = parse_config(config, workdir=workdir)

    deployment.deploy()

    for experiment in experiments:
        experiment.run(deployment)

    for result in results:
        result.output()

    deployment.teardown()


@main.command()
@click.option(
    "-c", "--config", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-d", "--workdir", required=True,
    type=click.Path(file_okay=False, resolve_path=True)
)
def teardown(config, workdir):
    # Search for the pickle file
    pickle_path = os.path.join(workdir, "deployment", "deployment.pkl")
    if os.path.exists(pickle_path):
        with open(pickle_path, "rb") as f:
            deployment = pickle.load(f)
            assert isinstance(deployment, Deployment)
    else:
        # Get deployment from the config if the pickle file doesn't exist
        deployment, _, _ = parse_config(config, workdir)
    
    pprint(deployment)
    deployment.teardown()


@main.command()
@click.option(
    "-c", "--config", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-d", "--workdir", required=True,
    type=click.Path(file_okay=False, resolve_path=True)
)
@click.option(
    "-n", "--name", required=False,
    type=str,
    default=None
)
def ssh(config, workdir, name):
    # Search for the pickle file
    pickle_path = os.path.join(workdir, "deployment", "deployment.pkl")
    if os.path.exists(pickle_path):
        with open(pickle_path, "rb") as f:
            deployment = pickle.load(f)
            assert isinstance(deployment, Deployment)
    else:
        # Get deployment from the config if the pickle file doesn't exist
        deployment, _, _ = parse_config(config, workdir)
    
    if name is None:
        target_node = deployment.dev_vm
    else:
        for node in deployment.nodelist:
            if node.name == name:
                target_node = node
                break
        else:
            raise ValueError(f"Node {name} not found")

    invoke.run(f"ssh -i {deployment.ssh_key} {deployment.ssh_user}@{target_node.public_ip}", pty=True)


@main.command()
@click.option(
    "-c", "--config", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-d", "--workdir", required=False,
    type=click.Path(file_okay=False, resolve_path=True),
    default=None
)
def deploy(config, workdir):
    deployment, _, _ = parse_config(config, workdir)
    pprint(deployment)
    deployment.deploy()


if __name__ == "__main__":
    main()


