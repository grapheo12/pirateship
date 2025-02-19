# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

import subprocess
import click
from util import sendRemoteFile, executeRemoteCommand, executeCommand
import container_util as cu
import yaml

@click.command()
@click.option(
    "-d", "--docker_ssh",
    default=2222,
    help="Docker SSH Port",
    type=click.INT
)
@click.option(
    "-c", "--deploy-config", required=True,
    default = "azure-tf/setups/wan_aci.yaml",
    help="Deployment Configuration. How many containers to deploy and where",
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
 "-s", "--deployment-name",
 default = "pirateship",
 type=click.STRING
)
@click.option(
   "-r", "--registry-name", 
    default="pirateship",
    help = "Container Registry",
    type=click.STRING
)
@click.option(
  "-g", "--resource-group",
  default = "pirateship-aci",
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
   "-l", "--local", 
    default=False,
    help = "When set to true, deploys containers locally",
    type=click.BOOL
)
def main(docker_ssh, deploy_config, deployment_name, registry_name, resource_group, image_name, cluster_key, local): 
   # This code assumes that the nodelist.txt format is: 
   # machine_tag (= deployment_name), ip
   with open("nodelist.txt", "r") as nodelist:
      lines =  nodelist.readlines()
      for line in lines:
         container_tag = line.split()[0]
         if not local:
           cu.deleteDeployment(resource_group,container_tag)
         else:
           executeCommand("docker kill " + container_tag)
           executeCommand("docker rm " + container_tag)
   #TODO(natacha): this step currently mirrors the steps taken
   # in azure-teardown.sh. Should make this more robust and
   # paramaterise file location.
   executeCommand("rm -f extra_nodelist.txt")
   executeCommand("rm -f nodelist_public.txt")
   executeCommand("rm -f nodelist.txt")



if __name__ == "__main__":
   main()
