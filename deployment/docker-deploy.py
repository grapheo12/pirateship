import subprocess

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

# (Helper) Execute a bash command in Python. Throws exception if error
def executeCommand(command):
    print("Calling " + command)
    subprocess.check_call(command, shell=True)

# (Helper) Sends file to remote host
def sendRemoteFile(local_file, user, h, remote_dir, key=None, port=None):
    port_string = "-P " + str(port)  + " " if port else " "
    if not key:
        cmd = "scp -o StrictHostKeyChecking=no  " + \
            local_file + " " + user + "@" + h + ":" + remote_dir
    else:
        cmd = "scp -o StrictHostKeyChecking=no -i " + key + \
            " " + port_string + local_file + " " + user + "@" + h + ":" + remote_dir
    executeCommand(cmd)

# (Helper) Executes command on remote host
def executeRemoteCommand(user, host, command, key=None, port=None):
    port_string = "-p " + str(port)  + " " if port else " "
    if not key:
        cmd = "ssh -o StrictHostKeyChecking=no -t "  + port_string +  user + "@" + host + " \"" + command + "\""
    else:
        cmd = "ssh -o StrictHostKeyChecking=no -t -i " + \
            key + " " + port_string + user + "@" + host + " \"" + command + "\""
    executeCommand(cmd)

def main(): 

   ssh_docker_port = 2222 #TODO: add to configuration. Hard code. This is the port on which the docker container ssh port will be exposed.
   server_port=3000 #TODO: add to configuration. Hard code. This is the port on which the server socket will listen.

   # Build containers
   image_name = "pftimage" 
   print("Building docker image {}".format(image_name))
   subprocess.call(["docker", "build", "-t", image_name, "docker/"]) #TODO: expects cluster_key.pub in same repo as called. FIX
   print("Docker image built")

   #TODO: deploy containers according to TF config. 
   local = True 
   if local:
     # Launch containers locally
     for i in range(0, 5): 
      container_name = "pftcontainer" + str(i)
      subprocess.call(["docker", "run", "-d", "-p", str(ssh_docker_port + i) + ":22",  "-p", str(server_port + i) + ":" + str(server_port), "--name", container_name, image_name])    
      print("Container {} launched.".format(container_name))

   # Once containers have been launched, extract IP addresses and configure nodelist.txt accordingly
   # TODO: generate nodelist.txt and nodelist_public.txt. Currently hard coded to localhost
   # TODO: run_remote_client_sweep.py currently expects clients/nodes in that order to link up the correct ssh_docker_port remapping. 

   # Prepare Dev environment on the client VM. This VM is used to compile and distribute the binaries, then sends them
   # to the other containers
   dev_vm_ip = "localhost"
   dev_user = "pftadmin"
   dev_ssh_key= "/home/ncrooks/.ssh/cluster_key.pem" #TODO: add to configuration. No hard coding

   sendRemoteFile( "__prepare-dev-env.sh", dev_user, dev_vm_ip, "~/", dev_ssh_key, ssh_docker_port)
   sendRemoteFile( "ideal_bashrc",dev_user, dev_vm_ip, "~/", dev_ssh_key, ssh_docker_port)   
   executeRemoteCommand(dev_user, dev_vm_ip,  "sh ~/__prepare-dev-env.sh", dev_ssh_key, ssh_docker_port)


   print("Dev environment prepared on client container")

if __name__ == "__main__":
   main()
