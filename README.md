# Introduction

The following is a demo Movie Recommendation System Data pipeline implemented in Azure Databricks. 

# Architecture

Event Hub ratings data is generated via a simple .NET core application running in an Azure Container instance. The ratings data is then ingested via a Spark Streaming (Scala) job within Azure Databricks. The recommendation system makes use of a collaborative filtering model, specifically the Alternating Least Squares (ALS) algorithm implemented in Spark ML and pySpark (Python). The solution also contains two scheduled jobs that demonstrates how one might run these in Production. The first job creates new recommendation for all users in the User table while the second job retrains the model with the newly received ratings data. The solution also demonstrates Sparks Model Persistence feature in which one can load a model in a different language (Scala) from what it was originally saved as (Python). Finally, the data is visualized with parameterize Notebook / Dashboard using Databricks Widgets.

*DISCLAIMER: The code base is not for Production purposes and is only for demonstration purposes.*

![Architecture](images/architecture.PNG?raw=true "Architecture")

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
     
# Data

This solutions makes use of the [MovieLens Dataset](https://movielens.org/)*

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
<p><small>*F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context. ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4, Article 19 (December 2015), 19 pages. DOI=http://dx.doi.org/10.1145/2827872</small></p>
