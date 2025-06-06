# Utility scriptsto launch confidential containers in Azure 


import subprocess
import yaml
from ssh_utils import executeCommand, executeCommandArgs
from os import cpu_count
from psutil import virtual_memory

# Generate fully qualified registry image name from 'base' image name
def getFullImageName(acrPrefix, imageName):
    return acrPrefix + ".azurecr.io/" + imageName

# Build docker Image (no argments expected)
def buildImage(fullImageName, dockerFolder, ssh_public_key):
    print("Building docker image {}".format(fullImageName))
    try: 
        print(executeCommand(f"cp {ssh_public_key} {dockerFolder}"))
        print(executeCommand("docker build -t " + fullImageName + " " + dockerFolder))
    # print(executeCommand("docker build --no-cache -t " + fullImageName + " " + dockerFolder))
        print("Docker image built")
    except subprocess.CalledProcessError as e:
        print("Error building docker image: ", e.stderr)


# Push Docker Image To the Registry. Note that the ACR Prefixi must be the full *.azurecr.io name
# and have been registered through the Azure container registry. 
def pushImage(acrPrefix, fullImageName, password=None):
    print("Pushing docker image {}".format(fullImageName))
    # Obtain Password if don't already have token
    if password is None:
        cmd = ["az", "acr", "login", "-n", acrPrefix,"--expose-token","--output", "tsv", "--query","accessToken"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        password = result.stdout
    cmd = "docker login -u 00000000-0000-0000-0000-000000000000 -p " + password + " " + acrPrefix + ".azurecr.io"
    print(executeCommand(cmd))
    print(executeCommand("docker push " + fullImageName))
    print("Docker image pushed")

# Log out from Docker
def dockerLogOut():
    print(executeCommand("docker logout"))


# Returns token to access the Azure Container Registry
def getAcrToken(acrPrefix):
    return executeCommand("az acr login --name " + acrPrefix + " --expose-token --output tsv --query accessToken")

# When running in confidential containers, need to populate the ccePolicy parameter 
# in the ARM Template.  The CCE policy is used for attestation. The tool takes the ARM 
# template as an input to generate the policy. The policy enforces the specific container
# images, environment variables, mounts, and commands, which can then be validated when the 
# container group starts up
def updateSku(template):
  # Use the Azure Confcom extension to generate the signature.
  # This call automatically updates the template
  cmd = "az confcom acipolicygen -a " + template
  print(executeCommand(cmd))

# Cancels deployment
def cancelDeployment(resourceGroup, deploymentName):
    print(executeCommandArgs(["az", "deployment", "group", "cancel",
                               "--resource-group",  resourceGroup,
                               " --name ", deploymentName]))

# Update the ARM template with the appropriate primary image, region, deployment etc. 
# and launch deployment
def launchDeployment(templateFile, resourceGroup, deploymentName, acrPrefix, primary_image, sshKey, acrToken, location, server_port, local, confidential, total_node_count, spot=True):
    print("Launching deployment")
    if not local:  
        if (confidential): 
            updateSku(templateFile)
        ssh = "ssh=\"" + executeCommand("cat " + sshKey) + "\""
        location = "location=" + location
        deploymentName = "name=" + deploymentName
        primary_image = "primary-image=" + getFullImageName(acrPrefix, primary_image) 
        acr_token = "acr-token=" + executeCommand("az acr login --name " + acrPrefix + " --expose-token --output tsv --query accessToken")
        port = "port=" + str(server_port)
        registry = "registry=" +  acrPrefix + ".azurecr.io"
        print(executeCommandArgs(["az", "deployment", "group", "create", 
                                  "--resource-group", resourceGroup,
                                  "--template-file", templateFile,
                                  "--parameters", ssh, 
                                  "--parameters", location,
                                  "--parameters", deploymentName,
                                  "--parameters", primary_image,
                                  "--parameters", port,
                                  "--parameters", registry,
                                  "--parameters", acr_token]))
    
    else: 
        # PirateShip is quite resource intensive... cpu and memory quotas can be a way to run clusters on a single local machine 
        # if you think this is outside the scope od aci deployment (which it is), I can take it away and just create a utility script that handles this
        print(executeCommand(f"docker run -d -p {str(server_port)}:22 --cpus={(cpu_count() - 0.5) / total_node_count:.2f} --memory={(virtual_memory().total - 500_000_000) // total_node_count}b --name {deploymentName} {primary_image}"))
        # print(executeCommand(f"docker run -d -p {str(server_port)}:22 --memory={(virtual_memory().total - 500_000_000) // total_node_count}b --name {deploymentName} {primary_image}"))
        print("Container {} launched.".format(deploymentName))

# Cancels deloyments
def deleteDeployment(resourceGroup, deploymentName, local=False):
    if not local:
        resource_group = " --resource-group " + resourceGroup
        name = " --name " + deploymentName
        print("Executing command: az deployment group delete " + resource_group + " " +  name)
        print(executeCommand("az deployment group delete " + resource_group + " " + name))
    else:
        print(executeCommandArgs(["docker", "rm", "-f", deploymentName]))

# Obtains the public Ip address of all currently running containers in a specific deployment
# and resource group
def obtainIpAddress(resourceGroup, deploymentName, local): 
    if not local: 
        result = executeCommand("az container show -g " + resourceGroup + " -n " + deploymentName + " --query ipAddress.ip -o tsv") 
        print(result) 
    else:
        # Get Internal IP Address of Docker Container Using Docker Inspect
        result = executeCommandArgs(["docker", "inspect", "-f",  "\'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}\'", deploymentName])
        result = result.strip("'")  # Remove single quotes around the IP address
    print("IP Address of " + deploymentName + " is " + result)
    return result 


# Extracts deployment configuration. Takes in a yaml file
# and returns a list of pairs where the first entry is the region
# and the next entry is the number of containers to deploy in that region
# TODO(natacha) add extra fields to describe the amount of resources that should be
# allocated to each container)
# TODO(natacha): standardise configuration such that same configuration 
# can be used for TF and container deployments
def extractConfig(configFile):
      with open(configFile, 'r') as stream:
         try:
               config = yaml.safe_load(stream)
               return config
         except yaml.YAMLError as exc:
               print(exc)
               return None


# Collects all public IP addresses in a resource group and returns them in a json format
def collectAllIpAddressJson(resourceGroup, local):
    if not local:
        result = executeCommand("az container list -g " + resourceGroup + " --query [].{Name:name,IP:ipAddress.ip} -o json")
    else:
        result = executeCommandArgs(["docker", "inspect", "-f", "\'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}\'", deploymentName])
        #TODO(natacha): Local Json not implemented yet
    print(result)
    return result

def liftCpuQuota(deploymentName):
    previous_quota = executeCommandArgs(["docker", "inspect", "-f", "\'{{.HostConfig.CpuQuota}}\'", deploymentName])
    executeCommandArgs(["docker", "update", "--cpus", str(cpu_count()), deploymentName])
    return previous_quota

def setCpuQuota(deploymentName, quota):
    executeCommandArgs(["docker", "update", "--cpus", str(quota), deploymentName])