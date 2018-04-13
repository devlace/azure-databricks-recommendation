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
# Description: Deploy Databricks cluster
#
# Usage: 
#
# Requirments:  
#

set -o errexit
set -o pipefail
set -o nounset
#set -o xtrace

# Set path
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

############
# FUNCTIONS

wait_for_run () {
    # See here: https://docs.azuredatabricks.net/api/latest/jobs.html#jobsrunresultstate
    declare mount_run_id=$1
    while : ; do
        life_cycle_status=$(databricks runs get --run-id $mount_run_id | jq -r ".state.life_cycle_state") 
        result_state=$(databricks runs get --run-id $mount_run_id | jq -r ".state.result_state")
        if [[ $result_state == "SUCCESS" || $result_state == "SKIPPED" ]]; then
            break;
        elif [[ $life_cycle_status == "INTERNAL_ERROR" ]]; then
            err_msg=$(echo run_status | jq -r ".state.state_message")
            echo "Error while running ${mount_run_id}: $err_msg"
            exit 1
        else 
            echo "Waiting for run ${mount_run_id} to finish..."
            sleep 2m
        fi
    done
}

cluster_exists () {
    declare cluster_name="$1"
    declare cluster=$(databricks clusters list | tr -s " " | cut -d" " -f2 | grep ^${cluster_name}$)
    if [[ -n $cluster ]]; then
        return 0; # cluster exists
    else
        return 1; # cluster does not exists
    fi
}

############
# USER WARNING

echo "!! -- WARNING --!!"
echo "This will re-upload and overwrite existing notebooks with the same names in the 'notebooks' folder. "
echo "This will also drop and reload data in all Tables."
read -p "Are you sure you want to continue? " -n 1 -r
echo    # (optional) move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1 # handle exits from shell or function but don't exit interactive shell
fi

############
# SETUP

# Create cluster, if not yet exists
cluster_config="./config/cluster.config.json"
cluster_name=$(cat $cluster_config | jq -r ".cluster_name")
if cluster_exists $cluster_name; then 
    echo "Cluster ${cluster_name} already exists!"
else
    echo "Creating cluster ${cluster_name}..."
    databricks clusters create --json-file $cluster_config
fi

# Upload notebooks, mount storage and setup up tables
echo "Uploading notebooks..."
databricks workspace import_dir "../../notebooks" "/recommender" --overwrite
echo "Mounting blob storage. This may take a while as cluster spins up..."
wait_for_run $(databricks runs submit --json-file "./config/run.mountstorage.config.json" | jq -r ".run_id" )
echo "Setting up tables. This may take a while as cluster spins up..."
wait_for_run $(databricks runs submit --json-file "./config/run.setuptables.config.json" | jq -r ".run_id" )

# Schedule and run jobs
databricks jobs run-now --job-id $(databricks jobs create --json-file "./config/job.scoremodel.config.json" | jq ".job_id")
databricks jobs run-now --job-id $(databricks jobs create --json-file "./config/job.refitmodel.config.json" | jq ".job_id")
databricks jobs run-now --job-id $(databricks jobs create --json-file "./config/job.ingestdata.config.json" | jq ".job_id")
