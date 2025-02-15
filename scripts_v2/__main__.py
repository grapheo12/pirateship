from collections import defaultdict
import multiprocessing
from time import sleep
import time
from typing import List
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

import tqdm
from crypto import *
from ssh_utils import *
from deployment import Deployment
from experiments import Experiment
from results import *
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


def nested_override(source, diff):
    target = deepcopy(source)
    for k, v in diff.items():
        if not (k in target):  # New key
            target[k] = deepcopy(v)
        elif not (isinstance(target[k], dict)):  # target[k] is not nested
            target[k] = deepcopy(v)
        # v is not nested: Hitting this condition means the dict is malformed (doesn't conform to a schema)
        elif not (isinstance(v, dict)):
            target[k] = deepcopy(v)
        else:  # Nested object
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


def print_dummy(experiments, **kwargs):
    pprint(experiments)
    print(kwargs)


@dataclass
class Result:
    plotter_func: Callable
    experiments: list
    kwargs: dict

    def __init__(self, fn_name, experiments, kwargs):
        self.plotter_func = print_dummy  # TODO: Port plotting functions
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
        node_config = nested_override(
            base_node_config, e.get("node_config", {}))
        client_config = nested_override(
            base_client_config, e.get("client_config", {}))
        controller_must_run = e.get("controller_must_run", False)
        git_hash_override = e.get("git_hash", None)
        project_home = toml_dict["project_home"]

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
                    _e.get("build_command", "make"),
                    git_hash_override,
                    project_home,
                    controller_must_run
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
                e.get("build_command", "make"),
                git_hash_override,
                project_home,
                controller_must_run
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
        experiment.deploy(deployment)

    deployment.copy_all_to_remote_public_ip()

    script_lines = []
    for experiment in experiments:
        _lines = experiment.run_plan()
        script_lines.extend(_lines)

    deployment.run_job_in_dev_vm(script_lines)

    deployment.sync_local_to_dev_vm()

    # for result in results:
    #     result.output()

    # deployment.teardown()


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

    invoke.run(
        f"ssh -i {deployment.ssh_key} {deployment.ssh_user}@{target_node.public_ip}", pty=True)


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
    "-cmd", "--command", required=True,
    type=str
)
def run_command(config, workdir, command):
    pickle_path = os.path.join(workdir, "deployment", "deployment.pkl")
    if os.path.exists(pickle_path):
        with open(pickle_path, "rb") as f:
            deployment = pickle.load(f)
            assert isinstance(deployment, Deployment)
    else:
        # Get deployment from the config if the pickle file doesn't exist
        deployment, _, _ = parse_config(config, workdir)

    print(f"Running command on {len(deployment.nodelist)} nodes")

    for node in deployment.nodelist:
        print(f"Running command on {node.name}")
        try:
            run_remote_public_ip([command], deployment.ssh_user,
                                deployment.ssh_key, node, hide=False)
        except Exception as e:
            pass


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


@main.command()
@click.option(
    "-c", "--config", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-d", "--workdir", required=True,
    type=click.Path(file_okay=False, resolve_path=True)
)
def deploy_experiments(config, workdir):
    # Try to get deployment from the pickle file
    pickle_path = os.path.join(workdir, "deployment", "deployment.pkl")
    with open(pickle_path, "rb") as f:
        deployment = pickle.load(f)
        assert isinstance(deployment, Deployment)

    # Try to get experiments from the pickle file
    experiments = []
    for root, _, files in os.walk(os.path.join(workdir, "experiments")):
        for f in files:
            if f == "experiment.pkl":
                with open(os.path.join(root, f), "rb") as f:
                    experiment = pickle.load(f)
                    assert isinstance(experiment, Experiment)
                    experiments.append(experiment)

    if len(experiments) == 0:
        _, experiments, _ = parse_config(config, workdir=workdir)

    for experiment in experiments:
        try:
            experiment.deploy(deployment)
        except Exception as e:
            print(f"Error deploying {experiment.name}. Continuing anyway: {e}")


    # Copy over the entire directory to all nodes
    deployment.copy_all_to_remote_public_ip()


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
def run_experiments(config, workdir, name):
    # Try to get deployment from the pickle file
    pickle_path = os.path.join(workdir, "deployment", "deployment.pkl")
    with open(pickle_path, "rb") as f:
        deployment = pickle.load(f)
        assert isinstance(deployment, Deployment)

    # Find all experiments pickle files recursively in workdir/experiments
    experiments = []
    for root, _, files in os.walk(os.path.join(workdir, "experiments")):
        for f in files:
            if f == "experiment.pkl":
                with open(os.path.join(root, f), "rb") as f:
                    experiment = pickle.load(f)
                    assert isinstance(experiment, Experiment)
                    experiments.append(experiment)

    script_lines = []
    for experiment in experiments:
        _script = experiment.run_plan()
        script_lines.extend(_script)

    deployment.run_job_in_dev_vm(script_lines)

    deployment.sync_local_to_dev_vm()


@main.command()
@click.option(
    "-c", "--config", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-d", "--workdir", required=True,
    type=click.Path(file_okay=False, resolve_path=True)
)
def status(config, workdir):
    # Try to get deployment from the pickle file
    pickle_path = os.path.join(workdir, "deployment", "deployment.pkl")
    with open(pickle_path, "rb") as f:
        deployment = pickle.load(f)
        assert isinstance(deployment, Deployment)

    print(deployment)

    # Get the status.txt from remote to find the current running job.
    try:
        status = run_remote_public_ip([
            "cat status.txt"
        ], deployment.ssh_user, deployment.ssh_key, deployment.dev_vm)[0]
        fname = status.split("\n")[0].strip().split(" ")[1]
        print("Current running job:", fname)
    except Exception as e:
        print("No job running")
        return

    # Read the job file
    num_cmds = 0
    job_path = os.path.join(workdir, "jobs", fname)
    try:
        with open(job_path) as f:
            job_lines = f.readlines()
            for line in job_lines:
                if line.startswith("sh"):
                    num_cmds += 1

        deployment.wait_till_end(num_cmds)
    except Exception as e:
        print("Error reading job file:", job_path, e)


@main.command()
@click.option(
    "-c", "--config", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-d", "--workdir", required=True,
    type=click.Path(file_okay=False, resolve_path=True)
)
def sync_local(config, workdir):
    # Try to get deployment from the pickle file
    pickle_path = os.path.join(workdir, "deployment", "deployment.pkl")
    with open(pickle_path, "rb") as f:
        deployment = pickle.load(f)
        assert isinstance(deployment, Deployment)

    print(deployment)

    deployment.sync_local_to_dev_vm()


@main.command()
@click.option(
    "-c", "--config", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-d", "--workdir", required=True,
    type=click.Path(file_okay=False, resolve_path=True)
)
def clean_dev(config, workdir):
    # Try to get deployment from the pickle file
    pickle_path = os.path.join(workdir, "deployment", "deployment.pkl")
    with open(pickle_path, "rb") as f:
        deployment = pickle.load(f)
        assert isinstance(deployment, Deployment)

    print(deployment)

    deployment.clean_dev_vm()


if __name__ == "__main__":
    main()
