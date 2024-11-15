# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

from collections import defaultdict
from sys import argv, exit, stdout
from pprint import pprint
from copy import deepcopy
import shutil
from os.path import exists

NUM_REGIONS = 4
NUM_RECONFIG_VMS = 8
NUM_ORIGINAL_VMS = 4

def read_nodelist(fname):
    vm_types = defaultdict(list)
    with open(fname, "r") as f:
        for line in f.readlines():
            if line.startswith("# "):
                line = line[2:]
            if line.startswith("clientpool"):
                vm_types["client"].append(line)
            else:
                for i in range(NUM_REGIONS):
                    loc = "loc" + str(i)
                    if loc in line:
                        vm_types[loc].append(line)
                        break
    
    return vm_types


def recreate_nodelist_txt(nodelist, file=stdout):
    if "client" in nodelist:
        print("".join(nodelist["client"]), file=file, end="")

    for i in range(NUM_REGIONS):
        loc = "loc" + str(i)
        if loc in nodelist:
            print("".join(nodelist[loc]), file=file, end="")

    print("", file=file)
             


if len(argv) != 3 or not(argv[1] in ["restore", "lan-reconfig"] + ["c" + str(i) for i in range(1, 6)]):
    print(f"Usage: python3 {argv[0]} <lan-reconfig | c1 | c2 | c3 | c4 | c5> <nodelist dir>")
    exit(0)

nodelist_dir = argv[2]
nodelist_path = f"{nodelist_dir}/nodelist.txt"
extra_nodelist_path = f"{nodelist_dir}/extra_nodelist.txt"
backup_nodelist_path = f"{nodelist_dir}/nodelist-bak.txt"

assert(exists(nodelist_path))

job = argv[1]

if job == "restore":
    assert(exists(backup_nodelist_path))
    shutil.copyfile(backup_nodelist_path, nodelist_path)
    exit(0)

nodelist = read_nodelist(nodelist_path)

c1_config = {
    "loc0": 1,
    "loc1": 1,
    "loc2": 1,
    "loc3": 1,
}

c2_config = {
    "loc0": 2,
    "loc1": 0,
    "loc2": 2,
    "loc3": 1,
}

c3_config = {
    "loc0": 2,
    "loc1": 1,
    "loc2": 1,
    "loc3": 1,
}

c4_config = {
    "loc0": 3,
    "loc1": 0,
    "loc2": 3,
    "loc3": 0,
}

c5_config = {
    "loc0": 3,
    "loc1": 0,
    "loc2": 3,
    "loc3": 2,
}

configs = {
    "c1": c1_config,
    "c2": c2_config,
    "c3": c3_config,
    "c4": c4_config,
    "c5": c5_config
}

shutil.copyfile(nodelist_path, backup_nodelist_path)

if job in ["c" + str(i) for i in range(1, 6)]:
    _config = configs[job]
    _nodelist = dict()

    for k, v in nodelist.items():
        if not (k in _config):
            _nodelist[k] = v
            continue

        _v = [a if j < _config[k] else "# " + a for j, a in enumerate(v)]
        _nodelist[k] = _v

    with open(nodelist_path, "w") as f:
        recreate_nodelist_txt(_nodelist, f)

else:
    # LAN reconfig
    # Needs extra_nodelist

    all_clients = nodelist["client"][:]
    all_nodes = []
    for i in range(NUM_REGIONS):
        loc = "loc" + str(i)
        if loc in nodelist:
            all_nodes.extend(nodelist[loc])


    controller = "controller\t" + all_clients[0].split("\t")[1]
    all_clients.append(controller)

    while len(all_nodes) < NUM_RECONFIG_VMS:
        all_nodes = all_nodes + all_nodes
    all_nodes = all_nodes[:NUM_RECONFIG_VMS]
    all_nodes = [a.split("\t")[0] + f"_tag{i}\t" + a.split("\t")[1] for i, a in enumerate(all_nodes)]

    with open(nodelist_path, "w") as f:
        print("".join(all_clients), end="", file=f)
        print("".join(all_nodes[:NUM_ORIGINAL_VMS]), end="", file=f)
        print("", file=f)

    with open(extra_nodelist_path, "w") as f:
        print("".join(all_nodes[NUM_ORIGINAL_VMS:]), end="", file=f)
        print("", file=f)



    