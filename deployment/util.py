## Helper Python Functions
## TODO(natacha): move to a util library

import subprocess

def executeCommandArgs(command):
    print("Executing command: ", command)
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode !=0: 
        ret = result.stderr
    else: 
        ret = result.stdout
    return ret.rstrip()

# (Helper) Execute a bash command in Python. Throws exception if error
def executeCommand(command):
    return executeCommandArgs(command.split())

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