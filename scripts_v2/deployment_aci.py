from copy import deepcopy
import os
from pprint import pprint
import pickle
import json
import time
from time import sleep
import tqdm
import re
import container_utils as cu
from ssh_utils import *
from deployment import Deployment


class AciDeployment(Deployment):
    '''
    This deploy targets Azure containers (both confidential and non-confidential) .
    ACI containers use docker for launch and can directly be SSH-ed into using public
    IPs on a pre-allocated port or on port 22.

    For other SSH-able deployments, override the following methods:
        - deploy
        - teardown
        - __init__
    '''
    def parse_custom_layouts(self):
        """
        Custom layout toml format:

        [[deployment_config.custom_layout]]
        name = "layout1"

        # Order of regions will come from terraform
        nodes_per_region = [1, 0, 0, 1]
        """
        self.custom_layouts = {}
        if not("custom_layout" in self.raw_config):
            return
        
        custom_layouts = self.raw_config["custom_layout"]
        assert isinstance(custom_layouts, list)


        for layout in custom_layouts:
            name = layout["name"]
            nodes_per_region = layout["nodes_per_region"]
            # clients_per_region = layout["clients_per_region"]
            assert isinstance(nodes_per_region, list)
            # assert isinstance(clients_per_region, list)

            self.custom_layouts[name] = {
                "nodes_per_region": nodes_per_region,
                # "clients_per_region": clients_per_region
            }


    def __init__(self, config, workdir):
        self.nodelist = []
        self.workdir = workdir
        self.raw_config = deepcopy(config)
        self.first_client = False
        self.mode = config["mode"]

        self.ssh_user = config["ssh_user"]
        if os.path.isabs(config["ssh_pub_key"]):
             # Extract final name of file, removing path
            ssh_key_name = os.path.basename(config["ssh_pub_key"])
            print(ssh_key_name)
            # Copy the key into workdir/deployment
            print(executeCommandArgs(["mkdir", "-p", os.path.join(workdir, "deployment")]))
            print(executeCommandArgs(["cp", config["ssh_pub_key"], os.path.join(workdir, "deployment",ssh_key_name)]))
            self.ssh_pub_key = os.path.join(workdir, "deployment", ssh_key_name)
            print(f"SSH KEY IS {self.ssh_pub_key}")
        else:
            self.ssh_pub_key = os.path.join(workdir, "deployment", config["ssh_pub_key"])

        if os.path.isabs(config["ssh_key"]):
             # Extract final name of file, removing path
            ssh_key_name = os.path.basename(config["ssh_key"])
            print(ssh_key_name)
            # Copy the key into workdir/deployment
            print(executeCommandArgs(["mkdir", "-p", os.path.join(workdir, "deployment")]))
            print(executeCommandArgs(["cp", config["ssh_key"], os.path.join(workdir, "deployment",ssh_key_name)]))
            self.ssh_key = os.path.join(workdir, "deployment", ssh_key_name)
            print(f"SSH KEY IS {self.ssh_key}")
        else:
            self.ssh_key = os.path.join(workdir, "deployment", config["ssh_key"])

        self.node_port_base = int(config["node_port_base"])

        # True if running inside a (confidential) container
        self.confidential = config["confidential"] 
        # Name of (unqualified) Azure Registry
        self.registry_name = config["registry_name"]
        # Name of Azure Resource Group  
        self.resource_group = config["resource_group"]
        # Name of Container Image
        self.image_name = config["image_name"]
        # Template Json necessary to construct container image
        if os.path.isabs(config["template"]):
            self.template= config["template"]
        else:
            self.template= os.path.join(workdir, "deployment", "azure-aci", config["template"])

        # Remapped Docker SSH Port for local container deployment
        self.docker_ssh = config["docker_ssh"]
        
        if self.mode == "local":
            # Offers opportunity to test/deploy containers locally. Key difference is that the containers listen
            # externally on an remapped port (docker_ssh). Otherwise works as normal.
            self.local = True
        else:
            self.local = False 

        if self.mode == "manual":
            # If manual, the nodelist must be specified in the config file
            self.populate_nodelist()

        self.parse_custom_layouts()

    def find_azure_aci_dir(self):
        '''
        Find where the yaml files are located.
        The search paths are hardcoded and are relative to the root of the repo.
        '''
        search_paths = [
            os.path.join("deployment", "azure-aci"),
            os.path.join("scripts_v2", "deployment", "azure-aci"),
        ]

        found_path = None
        for path in search_paths:
            if os.path.exists(path):
                found_path = os.path.abspath(path)
                break

        if found_path is None:
            raise FileNotFoundError("Azure ACI Deployment directory not found")
        else:
            print(f"Found Azure ACI Deployment directory at {found_path}")

        return found_path
    
    def prepare_dev_vm(self):
        '''
        Find where __prepare-dev-env.sh and ideal_bashrc are located.
        Copy them to the dev VM and run __prepare-dev-env.sh
        Search paths are hardcoded and are relative to the root of the repo.
        '''
        if self.dev_vm is None:
            raise ValueError("No dev VM found")
        
        # Find __prepare_dev_env.sh and ideal_bashrc
        search_paths = [
            "deployment",
            os.path.join("scripts_v2", "deployment"),
            os.path.join("deployment", "azure-aci"),
            os.path.join("scripts_v2", "deployment", "azure-aci"),
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

    # In ACI, the SSH private key must already exist. This function ensures that the key has the correct permissions
    # and is used for compatibility with Terraform 
    def get_ssh_key(self):
           run_local([
            "chmod 600 " + self.ssh_key
        ])

    # Generates container tag for client containers, where 
    # TODO(natacha) add parameter descritions
    # Note that _ is not an allowable character in Azure containers
    def generate_client_container_tag(self, global_idx, platform_idx, inregion_idx):
        tee_name = "aci" if not self.confidential else "caci"
        return f"clientpool-vm{global_idx}-{tee_name}-loc{platform_idx}-id{inregion_idx}"

    # Generates container tag for node containers, where 
    # TODO(natacha) add parameter descritions
    # Note that _ is not an allowable character in Azure containers
    def generate_node_container_tag(self, global_idx, platform_idx, inregion_idx):
        tee_name = "aci" if not self.confidential else "caci"
        return f"nodepool-vm{global_idx}-{tee_name}-loc{platform_idx}-id{inregion_idx}"

    # Current Shubham code uses "_" rather than "-", but this is not possible in Azure.
    # This function converts "_" to "-" in the container tag.
    # TODO(natacha): Let's move everything to a more consistent naming scheme.    
    def convert_to_terraform(tag):
        return tag.replace("-", "_")


    def deploy(self):

        print("[WARNING]: To run this script, the user must be logged in to the Azure CLI. Please make sure to run az login from command line first")

        run_local([
            f"mkdir -p {self.workdir}",
            f"mkdir -p {self.workdir}/deployment",
        ])
        with open(os.path.join(self.workdir, "deployment", "deployment.txt"), "w") as f:
            pprint(self, f)

        if self.mode == "manual":
            # Manual must mean there is a nodelist specified in the toml file.
            # There is no need to deploy.
            return

       # Find the azure-tf directory relative to where the script is being called from
        found_path = self.find_azure_aci_dir()

        if found_path is None:
            raise FileNotFoundError("Azure ACI directory not found")
        else:
            print(f"Found Azure ACI directory at {found_path}")

        # There must exist a configuration file in azure-aci/setups for the deployment mode
        deploy_config  = os.path.join(found_path, "setups", f"{self.mode}.yaml")
        print(deploy_config)
        if not os.path.exists(deploy_config):
            raise FileNotFoundError(f"Deployment config file for deployment mode {self.mode} not found")
        if not os.path.exists(self.template):
            raise FileNotFoundError(f"Deployment config file for Azure Template{self.template} not found")


        # Get token that will allow connectiong to the Azure Container Registry
        token = cu.getAcrToken(self.registry_name)

        # Build containers
        cu.buildImage(cu.getFullImageName(self.registry_name, self.image_name), found_path) #TODO: expects cluster_key.pub in same repo as called. FIX
        cu.pushImage(self.registry_name, cu.getFullImageName(self.registry_name, self.image_name))

        # Deploy containers on Azure or locally
        # If deploying on Azure, the YAML configuration file is used
        # to determine which/what containers to launch and where
        # TODO(natacha) Local Option is not currently implemented in the v2 scripts.

        docker_ssh = 22 if not self.local else docker_ssh # Reset SSH port to 22 if not in local mode
        extractConfigArgs = cu.extractConfig(deploy_config)['platforms']
        print(extractConfigArgs)

        # Iterate over the configuration (regions)
        total_idx = 0
        platform_idx = 0
        nodelist = {}
        for platform in extractConfigArgs:
          location = platform['location']
          print("Creating containers in " + location)
          # Launch Node Containers
          launch_count = int(platform["nodepool_count"])
          print("Launching "+ str(launch_count) + " node containers")
          base_port = self.node_port_base
          for i in range(0, launch_count):
            nodepool_container_tag_i = self.generate_node_container_tag(total_idx,platform_idx, i)
            #cu.launchDeployment(self.template, self.resource_group, nodepool_container_tag_i, self.registry_name, self.image_name, self.ssh_pub_key, token , location, base_port, docker_ssh, self.local, self.confidential)
            ip = cu.obtainIpAddress(self.resource_group, nodepool_container_tag_i, self.local)
            nodelist[nodepool_container_tag_i] = {
                "private_ip": "127.0.0.1" if self.local else ip ,
                "public_ip":  ip,
                "tee_type":  "aci" if not self.confidential else "caci",
                "region_id": platform_idx,
                "ssh_port": base_port
            }
            base_port = base_port + 1
            # This line is only relevant when running in local mode
            docker_ssh = docker_ssh + 1 if self.local else docker_ssh
            total_idx = total_idx + 1

          # Launch Client Containers
          launch_count = int(platform["clientpool_count"])
          print("Launching "+ str(launch_count) + " client containers")
          total_idx = 0
          for i in range(0, launch_count):
              client_container_tag_i = self.generate_client_container_tag(total_idx,platform_idx,i)
              #cu.launchDeployment(self.template, self.resource_group, client_container_tag_i, self.registry_name, self.image_name, self.ssh_pub_key, token , location, base_port, docker_ssh, self.local, self.confidential)
              ip = cu.obtainIpAddress(self.resource_group, client_container_tag_i, self.local)
              nodelist[client_container_tag_i] = {
                "private_ip":  ip,
                "public_ip":  ip,
                "tee_type":  "aci" if not self.confidential else "caci",
                "region_id": platform_idx,
                "ssh_port": base_port
              }
              base_port = base_port + 1
              # This line is only relevant when running in local mode
              docker_ssh = docker_ssh + 1 if self.local else docker_ssh
             
          platform_idx = platform_idx + 1
        
        self.raw_config["node_list"] = nodelist
        pprint(nodelist)

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


    def copy_all_to_remote_public_ip(self, only_to_dev_vm=False):
        # Use rsync to copy the entire directory to all nodes
        if not only_to_dev_vm:
            nodelist = self.nodelist[:]
        else:
            nodelist = [self.dev_vm]
        print(f"Copying {self.workdir} to {len(nodelist)} nodes")

        for node in nodelist:
            run_remote_public_ip([
                f"mkdir -p {self.workdir}",
            ], self.ssh_user, self.ssh_key, node)

        res = run_local([
            f"rsync -avz -e 'ssh -o StrictHostKeyChecking=no -i {self.ssh_key}' {self.workdir}/* {self.ssh_user}@{node.public_ip}:~/{self.workdir}/"
            for node in nodelist
        ], hide=True, asynchronous=True)

        for (i, node) in enumerate(nodelist):
            print("Copied to", node.name, "Output (truncated):\n", "\n".join(res[i].split("\n")[-2:]))

    def sync_local_to_dev_vm(self):
        # Use rsync to copy workdir from dev VM to local
        run_local([
            f"rsync -avz -e 'ssh -o StrictHostKeyChecking=no -i {self.ssh_key}' {self.ssh_user}@{self.dev_vm.public_ip}:~/{self.workdir}/* {self.workdir}/"
        ], hide=False)

    def clean_dev_vm(self):
        run_remote_public_ip([
            f"rm -rf {self.workdir}"
        ], self.ssh_user, self.ssh_key, self.dev_vm)


    def populate_raw_node_list_from_azure(self):
        found_path = self.find_azure_aci_dir()

        if found_path is None:
            raise FileNotFoundError("Azure Terraform directory not found")

        ips = cu.collectAllIpAddressJson(self.resource_group, self.local)
        print(ips)
        ips = {} if ips=="" else json.loads(ips)

        vm_names = {vm["Name"] for vm in ips}

        node_list = {}
        for name in vm_names:
            ip = next(vm["IP"] for vm in ips if vm["Name"] == name)
            
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
                "private_ip": ip,
                "public_ip": ip,
                "tee_type": tee_type,
                "region_id": region_id
            }

        self.raw_config["node_list"] = node_list

        pprint(node_list)




    def populate_nodelist(self):
        if self.mode != "manual":
            self.populate_raw_node_list_from_azure()
    
        first_client = False
        for name, info in self.raw_config["node_list"].items():
            print(name)
            print(info)
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
       
        
    def teardown(self):
        if self.mode == "manual":
            return
        
        print(cu.deleteDeployment(self.resource_group, self.deployment_name))

    def __repr__(self):
        s = f"Mode: {self.mode}\n"
        s += f"SSH user: {self.ssh_user}\n"
        s += f"SSH private key path: {self.ssh_key}\n"
        s += f"SSH public key path: {self.ssh_pub_key}\n"
        s += f"Node port base: {self.node_port_base}\n"
        s += "++++ Nodelist +++++\n"
        for node in self.nodelist:
            s += f"{node}\n"

        s += "++++ Custom layouts +++++\n"
        for name, layout in self.custom_layouts.items():
            s += f"{name}: {layout}\n"

        return s
    
    def get_all_node_vms(self):
        return [
            vm for vm in self.nodelist if "node" in vm.name
        ]
    
    def get_all_client_vms(self):
        return [
            vm for vm in self.nodelist if "client" in vm.name
        ]
    
    def get_all_client_vms_in_region(self, loc: int):
        return [
            vm for vm in self.get_all_client_vms() if vm.region_id == loc
        ]
    
    def get_nodes_with_tee(self, tee):
        return [
            vm for vm in self.get_all_node_vms() if tee in vm.tee_type
        ]
    
    def get_wan_setup(self, layout):
        layout = self.custom_layouts[layout]
        nodes_per_region = layout["nodes_per_region"]
        curr_vms_per_region = [0] * len(nodes_per_region)
        vms = []
        for node in self.get_all_node_vms():
            if curr_vms_per_region[node.region_id] < nodes_per_region[node.region_id]:
                vms.append(node)
                curr_vms_per_region[node.region_id] += 1

        return vms

    
    def wait_till_end(self, num_cmds: int):
        status = ""
        total_lines = num_cmds + 2
        with tqdm.tqdm(total=total_lines) as pbar:
            while not("Done" in status):
                sleep(1)
                old_status = len(status.split("\n"))
                try:
                    status = run_remote_public_ip([
                        f"cat status.txt"
                    ], self.ssh_user, self.ssh_key, self.dev_vm)[0]
                except Exception as e:
                    continue
                new_status = len(status.split("\n"))
                pbar.update(new_status - old_status)

                try:
                    screen_status = run_remote_public_ip([
                        f"tmux list-sessions"
                    ], self.ssh_user, self.ssh_key, self.dev_vm)[0]
                except Exception as e:
                    pbar.display(f"Failed to get status: {e}")
                    screen_status = ""

                if "no server running" in screen_status:
                    pbar.display("Tmux session ended", pos=2)
                    # break
        
        print("Experiment ended")

    
    def run_job_in_dev_vm(self, cmds, wait_till_end=True):
        curr_unix_time = int(time.time())
        job_file = f"job_{curr_unix_time}.sh"
        job_dir = os.path.join(self.workdir, "jobs")
        run_local(["mkdir -p " + job_dir])

        job_path = os.path.join(job_dir, job_file)
        cmd = "\n\n".join([f"""{x} || true
echo "Command {i} exited with status $?" >> status.txt
""" for i, x in enumerate(cmds)])
        script = f"""#!/bin/bash
set -e
set -o xtrace

# This script is generated by the experiment pipeline. DO NOT EDIT.

echo "Running {job_file}" > status.txt

{cmd}

echo "Done {job_file}" >> status.txt
"""
        with open(job_path, "w") as f:
            f.write(script)

        # Copy job dir to dev VM
        self.copy_all_to_remote_public_ip(only_to_dev_vm=True)

        session_name = job_file.replace(".", "_")

        # Run the script in a screen session in dev VM
        ssh_cmd = f"tmux new-session -d -s {session_name} 'sh {self.workdir}/jobs/{job_file} > arbiter_log.txt 2>&1'"
        res = run_remote_public_ip([
            ssh_cmd + " && echo 'Tmux session started'"
        ], self.ssh_user, self.ssh_key, self.dev_vm, hide=True)

        print(ssh_cmd, res)

        if wait_till_end:
            self.wait_till_end(len(cmds))
            
