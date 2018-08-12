# Introduction

The following is a Movie Recommendation System Data pipeline implemented within [Azure Databricks](https://azure.microsoft.com/en-au/services/databricks/). This solution aims to demonstrate Databricks as a Unified Analytics Platform by showing an end-to-end data pipeline, including an ETL data loading process, ingesting succeeding data through Spark Structured Streaming, model training and persisting trained model, productionizing fitted model through batch jobs, and finally, a parameterized user-dashboards all built on Azure Databricks. 

# Architecture

Movie ratings data is generated via a simple .NET core application running in an [Azure Container instance](https://azure.microsoft.com/en-gb/services/container-instances/) which sends this data into an [Azure Event Hub](https://azure.microsoft.com/en-gb/services/event-hubs/). The movie ratings data is then consumed and processed by a [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) (Scala) job within Azure Databricks. The recommendation system makes use of a collaborative filtering model, specifically the [Alternating Least Squares (ALS) algorithm](https://spark.apache.org/docs/2.3.0/ml-collaborative-filtering.html) implemented in Spark ML and pySpark (Python). The solution also contains two scheduled jobs that demonstrates how one might productionize the fitted model. The first job creates daily top 10 movie recommendations for all users while the second job retrains the model with the newly received ratings data. The solution also demonstrates Sparks [Model Persistence](https://spark.apache.org/docs/latest/ml-pipeline.html#ml-persistence-saving-and-loading-pipelines) in which one can load a model in a different language (Scala) from what it was originally saved as (Python). Finally, the data is visualized with a parameterize Notebook / Dashboard using [Databricks Widgets](https://docs.azuredatabricks.net/user-guide/notebooks/widgets.html#widget-api).

*DISCLAIMER: Code is not designed for Production and is only for demonstration purposes.*

![Architecture](images/architecture.PNG?raw=true "Architecture")

## Dashboard

The following shows the Movie Recommendations dashboard by User Id.

![Dashboard](images/dashboard.PNG?raw=true "Dashboard")

To access the Dashboard, go to `Workspace > recommender_dashboard > 07_user_dashboard` then select `View > User Recommendation Dashboard`

# Deployment

Ensure you are in the root of the repository and logged in to the Azure cli by running `az login`.

## Requirements

- [Azure CLI 2.0](https://azure.github.io/projects/clis/)
- [Python virtualenv](http://docs.python-guide.org/en/latest/dev/virtualenvs/) 
- [jq tool](https://stedolan.github.io/jq/download/)
- Check the requirements.txt for list of necessary Python packages. (will be installed by `make requirements`)
  
  Or Alternatively, you can use the Docker to deploy the solution in whcih case, you only need Docker.


## Development environment

- The following works with [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
- Clone this repository
- `cd azure-databricks-recommendation`
- `virtualenv .`  This creates a python virtual environment to work in.
- `source bin/activate`  This activates the virtual environment.
- `make requirements`. This installs python dependencies in the virtual environment.
- WARNING: The line endings of the two shell scripts `deploy.sh` and `configure_databricks.sh` may cause errors in your interpreter. You can change the line endings by opening the files in VS Code, and changing in the botton right of the editor.

## Deploy Entire Solution

- To deploy the solution, simply run `make deploy` and fill in the prompts. 
    - Alternative, you can also use the following docker container to deploy the solution:
        - `docker run -it devlace/azdatabricksrecommend`
    - Or build and run the container locally with:
        - `make deploy_w_docker`
- When prompted for a Databricks Host, enter the full name of your databricks workspace host, e.g. `https://southeastasia.azuredatabricks.net` 
- When prompted for a token, you can [generate a new token](https://docs.databricks.com/api/latest/authentication.html) in the databricks workspace.
- To view additional make commands run `make`

# Data

This solutions makes use of the [MovieLens Dataset](https://movielens.org/)*

# Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make deploy`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   │
    │   └── raw            <- The original, immutable data dump.
    ├── deploy             <- Deployment artifacts
    │   │
    │   └── databricks     <- Deployment artifacts in relation to the Databricks workspace
    │   │
    │   └── deploy.sh      <- Deployment script to deploy all Azure Resources
    │   │
    │   └── azuredeploy.json <- Azure ARM template w/ .parameters file
    │   │
    │   └── Dockerfile     <- Dockerfile for deployment
    │
    ├── notebooks          <- Azure Databricks Jupyter notebooks. 
    │
    ├── references         <- Contains the powerpoint presentation, and other reference materials.
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── src                <- Source code for use in this project.
        ├── __init__.py    <- Makes src a Python module
        │
        ├── data           <- Scripts to download or generate data
        │
        └── EventHubGenerator  <- Visual Studio solution EventHub Data Generator (Ratings)

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
<p><small>*F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context. ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4, Article 19 (December 2015), 19 pages. DOI=http://dx.doi.org/10.1145/2827872</small></p>
