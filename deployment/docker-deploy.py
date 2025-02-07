# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

import subprocess
import click
from util import sendRemoteFile, executeRemoteCommand, executeCommand
from launch_container import buildImage



@click.command()
@click.option(
    "-d", "--docker_ssh",
    default=2222,
    help="Docker SSH Port",
    type=click.INT
)
@click.option(
    "-c", "--deploy-config", required=True,
    help="Deployment Configuration. How many containers to deploy and where",
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
   "-l", "--local", 
    default=False,
    help = "When set to true, deploys containers locally",
    type=click.BOOL
)
def main(docker_ssh, deploy_config, local): 

   server_port=3000 #TODO: add to configuration. Hard code. This is the port on which the server socket will listen.

   # Build containers
   image_name = "pftimage" 
   buildImage(image_name, "docker/") #TODO: expects cluster_key.pub in same repo as called. FIX
   print("Docker image built")

   public_ips = {} 
   private_ips = {} 
   if local:
     # Launch containers locally
     for i in range(0, 5): 
      container_name = "pftcontainer" + str(i)
      #subprocess.call(["docker", "run", "-d", "-p", str(docker_ssh + i) + ":22",  "-p", str(server_port + i) + ":" + str(server_port + i), "--name", container_name, image_name])    
      subprocess.call(["docker", "run", "-d", "-p", str(docker_ssh + i) + ":22",  "--name", container_name, image_name])    
      print("Container {} launched.".format(container_name))
   else:
      print("Unimplemented")
   # Once containers have been launched, extract IP addresses and configure nodelist.txt accordingly
   # TODO: generate nodelist.txt and nodelist_public.txt. Currently hard coded to localhost

   # Prepare Dev environment on the client VM. This VM is used to compile and distribute the binaries, then sends them
   # to the other containers
   dev_vm_ip = "localhost"
   dev_user = "pftadmin"
   dev_ssh_key= "../../cluster_key.pem" #TODO: add to configuration. No hard coding

   sendRemoteFile( "__prepare-dev-env.sh", dev_user, dev_vm_ip, "~/", dev_ssh_key, docker_ssh)
   sendRemoteFile( "ideal_bashrc",dev_user, dev_vm_ip, "~/", dev_ssh_key, docker_ssh)   
   executeRemoteCommand(dev_user, dev_vm_ip,  "sh ~/__prepare-dev-env.sh", dev_ssh_key,docker_ssh)


   print("Dev environment prepared on client container")





if __name__ == "__main__":
   main()
