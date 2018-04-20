# Introduction

The following is a demo Movie Recommendation System Data pipeline implemented in Azure Databricks. 

# Architecture
![Architecture](/images/architecture.PNG?raw=true "Architecture")

Project Organization
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
    │
    ├── notebooks          <- Azure Databricks Jupyter notebooks. 
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
     

# Deployment

## Requirements

- [Azure CLI 2.0](https://azure.github.io/projects/clis/)
- Check the requirements.txt for list of necessary Python packages. 

## Deploy

Ensure you are in the root of the repository and logged in to the Azure cli by running `az login`.

- To deploy the solution, simply run `make deploy` and fill in the prompts.
- To download required Python packages, run `make requirements`
- To view additional make commands run `make`

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
