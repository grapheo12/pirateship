import subprocess
import sys
#import yaml
from util import executeCommand

# Generate fully qualified registry image name from 'base' image name
def getFullImageName(acrPrefix, imageName):
    return acrPrefix + ".azurecr.io/" + imageName

# Build docker Image (no argments expected)
def buildImage(fullImageName, dockerFolder):
    print("Building docker image {}".format(fullImageName))
    try: 
     executeCommand("docker build -t " + fullImageName + " " + dockerFolder) 
     print("Docker image built")
    except subprocess.CalledProcessError as e:
        print("Error building docker image: ", e.stderr)


# Push Docker Image To the Registry. Note that the ACR Prefixi must be the full *.azurecr.io name
# and have been registered through the Azure container registry. 
def pushImage(acrPrefix, fullImageName):
    print("Pushing docker image {}".format(fullImageName))
    # Obtain Password
    cmd = "az acr login --name " + acrPrefix +  " --expose-token --output tsv --query accessToken"
    password = executeCommand(cmd)
    cmd = "docker login -u 00000000-0000-0000-0000-000000000000 -p " + password + " " + acrPrefix + ".azurecr.io"
    executeCommand(cmd) 
    executeCommand("docker push " + fullImageName)
    print("Docker image pushed")

# When running in confidential containers, need to populate the ccePolicy parameter 
# in the ARM Template.  The CCE policy is used for attestation. The tool takes the ARM 
# template as an input to generate the policy. The policy enforces the specific container
# images, environment variables, mounts, and commands, which can then be validated when the 
# container group starts up
def updateSku(template):
  # Use the Azure Confcom extension to generate the signature.
  # This call automatically updates the template
  cmd = "az confcom acipolicygen -a " + template
  executeCommand(cmd)

# Update the ARM template with the appropriate primary image, region, deployment etc. 
# and launch deployment
def lauchDeployment(templateFile, resourceGroup, deploymentName, acrPrefix, fullPrimaryImage, sshKey, acrToken, location):
    resource_group = " --resource-group " + resourceGroup
    template = " --template-file " + templateFile
    name = " --parameters name " + deploymentName
    ssh = " --parameters ssh= " + executeCommand("cat " + sshKey)
    primary_image = " --parameters primary-image= " + fullPrimaryImage
    sidecar_image = " --parameters sidecar-image= " + acrPrefix + ".azure.cr.io/attestation-sidecar"
    acr_token = " --parameters acr_token= " + executeCommand("az acr login --name " + acrPrefix + " --expose-token --output tsv --query accessToken")
    region = " --location " + location
    executeCommand("az deployment group create" + resource_group + template + name + ssh + primary_image + sidecar_image + acr_token + region)

def deleteDeployment(resourceGroup, deploymentName):
    resource_group = " --resource-group " + resourceGroup
    name = " --name " + deploymentName
    executeCommand("az deployment group delete" + resource_group + name)

# Obtains the public Ip address of all currently running containers in a specific deployment
# and resource group
def obtainIpAddress(resourceGroup, deploymentName): 
    result = executeCommand("az container show -g " + resourceGroup + " -n " + deploymentName + " --query ipAddress.ip -o tsv") 
    print(result) 
    return result 
