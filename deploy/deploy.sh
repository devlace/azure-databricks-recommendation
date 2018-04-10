
timestamp=$(date +%s)
rgName="${1}"
rgLocation="${2}"
deployName="deployment${timestamp}"

# Deploy resource group
az group create --name "$rgName" --location "$rgLocation"

# Deploy resources
az group deployment create \
    --name "$deployName" \
    --resource-group "$rgName" \
    --template-file "./azuredeploy.json" \
    --parameters @"./azuredeploy.parameters.json" \
    --output json


# 