## Helper Python Functions
## TODO(natacha): move to a util library

import subprocess

# (Helper) Execute a bash command in Python. Throws exception if error
def executeCommand(command):
    print("Calling " + command)
    subprocess.check_call(command, shell=True)

# (Helper) Sends file to remote host
def sendRemoteFile(local_file, user, h, remote_dir, key=None, port=None):
    port_string = "-P " + str(port)  + " " if port else " "
    if not key:
        cmd = "scp -O -o StrictHostKeyChecking=no  " + \
            local_file + " " + user + "@" + h + ":" + remote_dir
    else:
        cmd = "scp -O -o StrictHostKeyChecking=no -i " + key + \
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