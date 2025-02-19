# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

import subprocess
import click
from util import sendRemoteFile, executeRemoteCommand, executeCommand, executeRemoteCommand2
import container_util as cu
import yaml

@click.command()

@click.option(
    "-c", "--deploy-config", required=True,
    default = "azure-tf/setups/wan_aci.yaml",
    help="Deployment Configuration. How many containers to deploy and where",
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
   "-r", "--registry-name", 
    default="ncrookspirateship",
    help = "Container Registry",
    type=click.STRING
)
@click.option(
  "-g", "--resource-group",
  default = "ncrooks-pirateship",
  help = "Azure Resource Group",
  type=click.STRING
)
@click.option(
     "-i", "--image-name",
     default = "pftimage", 
     help = "Image Name (unqualified)",
     type=click.STRING
)
@click.option(
    "-k", "--cluster-key",
    default = "docker/cluster_key.pub",
    help = "Cluster Public Key",
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-K", "--cluster-priv-key",
    default = "../../cluster_key.pem",
    help = "Cluster Public Key",
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
   "-l", "--local", 
    default=False,
    help = "When set to true, deploys containers locally",
    type=click.BOOL
)
@click.option(
   "-p", "--base-port",
   default = 3001, #TODO(natacha): should not be hard coded. Add to config
   help = "Starting Server Socket Port on which nodes will listen"
)
@click.option(
    "-d", "--docker_ssh",
    default=2222,
    help="Docker SSH Port",
    type=click.INT
)
@click.option(
   "-s", "--confidential",
   default = False,
   help = "When set to true, deploys containers with confidential computing",
   type = click.BOOL
)
def main(deploy_config, registry_name, resource_group, image_name, cluster_key, cluster_priv_key, base_port, docker_ssh, local, confidential): 

   print("[WARNING]: To run this script, the user must be logged in to the Azure CLI. Please make sure to run az login from command line first")
   
   # Get token that will allow connectiong to the Azure Container Registry
   token = cu.getAcrToken(registry_name)

   # Build containers
   cu.buildImage(cu.getFullImageName(registry_name, image_name), "docker/") #TODO: expects cluster_key.pub in same repo as called. FIX
   cu.pushImage(registry_name, cu.getFullImageName(registry_name, image_name))

   # Deploy containers on Azure or locally (if -l flag is configured)
   # If deploying on Azure, the YAML configuration file is used
   # to determine which/what containers to launch and where

   public_ips = {} 
   private_ips = {} 

   client_container_tag = "clientpool-vm"
   nodepool_container_tag = "nodepool-vm"
   docker_ssh = 22 if not local else docker_ssh # Reset SSH port to 22 if not in local mode
   extractConfigArgs = cu.extractConfig(deploy_config)['platforms']
   print(extractConfigArgs)

   # Iterate over the configuration (regions)
   for platform in extractConfigArgs:
      location = platform['location']
      print("Creating containers in " + location)
      # Launch Node  Containers
      launch_count = int(platform["nodepool_count"])
      print("Launching "+ str(launch_count) + " node containers")
      for i in range(0, launch_count): 
        nodepool_container_tag = nodepool_container_tag + str(i)
        cu.launchDeployment("docker/arm-template-standard.json", resource_group, nodepool_container_tag, registry_name, image_name, "docker/cluster_key.pub", token , location, base_port, docker_ssh, local, confidential) 
        ip = cu.obtainIpAddress(resource_group, nodepool_container_tag, local) 
        public_ips[nodepool_container_tag] = "127.0.0.1" if local else ip 
        private_ips[nodepool_container_tag] = ip
        # Code currently assumes that each new node listens on port 3000 + i,
        # starting from i = 1
        # TODO(natacha) Fix: this is very britle
        base_port = base_port + 1
        # This line is only relevant when running in local mode
        docker_ssh = docker_ssh + 1 if local else docker_ssh

      # Launch Client Containers
      location = platform['location']
      launch_count = int(platform["clientpool_count"])
      print("Launching "+ str(launch_count) + " client containers")
      for i in range(0, launch_count): 
        client_container_tag = client_container_tag + str(i)
        cu.launchDeployment("docker/arm-template-standard.json", resource_group, client_container_tag, registry_name, image_name, "docker/cluster_key.pub", token , location, base_port, docker_ssh, local, confidential) 
        ip = cu.obtainIpAddress(resource_group, client_container_tag, local) 
        public_ips[client_container_tag] = ip
        private_ips[client_container_tag] = ip
        # Code currently assumes that each new node listens on port 3000 + i,
        # starting from i = 1
        # TODO(natacha) Fix: this is very britle
        base_port = base_port + 1
        # This line is only relevant when running in local mode
        docker_ssh = docker_ssh + 1 if local else docker_ssh  

    

   # Once containers have been launched, extract IP addresses and configure nodelist.txt accordingly
   # TODO(natacha): names of config currently hard coded. Fix.
   with open("nodelist.txt", "w") as fi:
      for name,ip in private_ips.items():
         fi.write(name + " " + ip + "\n") 
   with open("nodelist_public.txt", "w") as fi:
      for name,ip in public_ips.items():
         fi.write(name + " " + ip + "\n") 

   # Prepare Dev environment on the client VM. This VM is used to compile and distribute the binaries, then sends them
   # to the other containers
   # TODO(natacha): these parameters are currently hard-coded. Fix;
   dev_vm_ip = public_ips["clientpool-vm0"] 
   dev_user = "pftadmin"

   sendRemoteFile( "__prepare-dev-env.sh", dev_user, dev_vm_ip, "~/", cluster_priv_key, docker_ssh)
   sendRemoteFile( "ideal_bashrc",dev_user, dev_vm_ip, "~/", cluster_priv_key, docker_ssh)   
  # print(executeRemoteCommand(dev_user, dev_vm_ip, "chmod +x ./__prepare-dev-env.sh", cluster_priv_key, docker_ssh))
   print(executeRemoteCommand(dev_user, dev_vm_ip, "./__prepare-dev-env.sh", cluster_priv_key, docker_ssh))
   


   print("Dev environment prepared on client container")





if __name__ == "__main__":
   main()
