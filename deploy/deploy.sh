

#!/bin/bash

# Access granted under MIT Open Source License: https://en.wikipedia.org/wiki/MIT_License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated 
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation 
# the rights to use, copy, modify, merge, publish, distribute, sublicense, # and/or sell copies of the Software, 
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions 
# of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED 
# TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF 
# CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
# DEALINGS IN THE SOFTWARE.
#
#
# Description: Deploy ARM template which creates a Databricks account
#
# Usage: ./deploy.sh myResourceGroup "East US 2"
#
# Requirments:  
# - User must be logged in to the az cli with the appropriate account set.
# - User must have appropraite permission to deploy to a resource group
# - User must have appropriate permission to create a service principal

set -o errexit
set -o pipefail
set -o nounset
#set -o xtrace

###################
# SETUP

# Check if required utilities are installed
command -v jq >/dev/null 2>&1 || { echo >&2 "I require jq but it's not installed. See https://stedolan.github.io/jq/.  Aborting."; exit 1; }
command -v az >/dev/null 2>&1 || { echo >&2 "I require azure cli but it's not installed. See https://bit.ly/2Gc8IsS. Aborting."; exit 1; }

# Globals
timestamp=$(date +%s)
deploy_name="deployment${timestamp}"
env_file="../.env"

# Constants
RED='\033[0;31m'
ORANGE='\033[0;33m'
NC='\033[0m'

# Set path
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

# Check if user is logged in
[[ -n $(az account show 2> /dev/null) ]] || { echo "Please login via the Azure CLI: "; az login; }

###################
# USER PARAMETERS

rg_name="${1-}"
rg_location="${2-}"
sub_id="${3-}"

while [[ -z $rg_name ]]; do
    read -rp "$(echo -e ${ORANGE}"Enter Resource Group name: "${NC})" rg_name
done

while [[ -z $rg_location ]]; do
    read -rp "$(echo -e ${ORANGE}"Enter Azure Location (ei. EAST US 2): "${NC})" rg_location
done

while [[ -z $sub_id ]]; do
    # Check if user only has one sub
    sub_count=$(az account list --output json | jq '. | length')
    if (( $sub_count != 1 )); then
        az account list
        read -rp "$(echo -e ${ORANGE}"Enter Azure Subscription Id you wish to deploy to (enter to use Default): "${NC})" sub_id
    fi
    sub_id=$(az account show --output json | jq -r '.id')
done

# Set account
echo "Deploying to Subscription: $sub_id"
az account set --subscription $sub_id

#####################
# Deploy ARM template

echo "Creating resource group: $rg_name"
az group create --name "$rg_name" --location "$rg_location"

echo "Deploying resources into $rg_name"
arm_output=$(az group deployment create \
    --name "$deploy_name" \
    --resource-group "$rg_name" \
    --template-file "./azuredeploy.json" \
    --parameters @"./azuredeploy.parameters.json" \
    --output json)

if [[ -z $arm_output ]]; then
    echo >&2 "ARM deployment failed." 
    exit 1
fi

#####################
# Ask user to configure databricks cli
dbi_workspace=$(echo $arm_output | jq -r '.properties.outputs.dbricksWorkspaceName.value')
echo -e "${ORANGE}"
echo "Configure your databricks cli to connect to the newly created Databricks workspace: ${dbi_workspace}. See here for more info: https://bit.ly/2GUwHcw."
databricks configure --token
echo -e "${NC}"

#####################
# Append to .env file

echo "Retrieving configuration information from newly deployed resources."

# Databricks details
dbricks_location=$(echo $arm_output | jq -r '.properties.outputs.dbricksLocation.value')
dbi_token=$(awk '/token/ && NR==3 {print $0;exit;}' ~/.databrickscfg | cut -d' ' -f3)
[[ -n $dbi_token ]] || { echo >&2 "Databricks cli not configured correctly. Please run databricks configure --token. Aborting."; exit 1; }

# Retrieve storage account details
storage_account=$(echo $arm_output | jq -r '.properties.outputs.storAccountName.value')
storage_account_key=$(az storage account keys list \
    --account-name $storage_account \
    --resource-group $rg_name \
    --output json |
    jq -r '.[0].value')

# Retrieve eventhub details
ehns_name=$(echo $arm_output | jq -r '.properties.outputs.eventhubsNsName.value')
eh_ratings_name=$(echo $arm_output | jq -r '.properties.outputs.eventhubsRatingsName.value')
eh_ratings_key=$(az eventhubs eventhub authorization-rule keys list \
    --namespace-name $ehns_name \
    --eventhub-name $eh_ratings_name \
    --name manage \
    --resource-group $rg_name \
    --output json |
    jq -r '.primaryKey')

eh_users_name=$(echo $arm_output | jq -r '.properties.outputs.eventhubUsersName.value')
eh_users_key=$(az eventhubs eventhub authorization-rule keys list \
    --namespace-name $ehns_name \
    --eventhub-name $eh_users_name \
    --name manage \
    --resource-group $rg_name \
    --output json |
    jq -r '.primaryKey')


# Build .env file
echo "Appending configuration to .env file."
echo "" >> $env_file
echo "" >> $env_file
echo "# ------ Configuration from deployment ${deploy_name} -----------" >> $env_file
echo "BLOB_STORAGE_ACCOUNT=${storage_account}" >> $env_file
echo "BLOB_STORAGE_KEY=${storage_account_key}" >> $env_file
echo "EVENTHUB_NAMESPACE=${ehns_name}" >> $env_file
echo "EVENTHUB_RATINGS=${eh_ratings_name}" >> $env_file
echo "EVENTHUB_RATINGS_KEY=${eh_ratings_key}" >> $env_file
echo "EVENTHUB_USERS=${eh_users_name}" >> $env_file
echo "EVENTHUB_USERS_KEY=${eh_users_key}" >> $env_file
echo "DBRICKS_DOMAIN=${dbricks_location}.azuredatabricks.net" >> $env_file
echo "DBRICKS_TOKEN=${dbi_token}" >> $env_file
echo "# --------------------------------------------------------------" >> $env_file