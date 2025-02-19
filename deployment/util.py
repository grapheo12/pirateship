## Helper Python Functions
## TODO(natacha): move to a util library

import subprocess
import sys
# from fabric import Connection

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
    return executeCommand(cmd)

# (Helper) Executes command on remote host
def executeRemoteCommand(user, host, command, key=None, port=None):
    if not key:
        if port:
          cmd = ["ssh","-o", "StrictHostKeyChecking=no", "-t " , "-p",  str(port), user + "@" + host, "\"" + command + "\""]
        else: 
          cmd = ["ssh","-o", "StrictHostKeyChecking=no", "-t ", user + "@" + host, "\"" + command + "\""]
    else:
        if port: 
          cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-t", "-i", 
            key, "-p", str(port), user + "@" + host,  "\"" + command + "\""]
        else:
          cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-t", "-i", 
                key, user + "@" + host,  "\"" + command + "\""]
    print("Executing remote command: ", cmd)
    result = subprocess.run(cmd, capture_output=True, text=True)
    print ("Result: ", result)
    if result.returncode !=0: 
        ret = result.stderr
    else: 
        ret = result.stdout
    return ret.rstrip()

def executeRemoteCommand2(user, host, command, key=None, port=22):
    conn = fabric.Connection(
            host=host,
            user=user,
            connect_kwargs= None if key is None else {
                "key_filename": key 
            },
            port = port
          )
    result = conn.run(command)
    print(result)