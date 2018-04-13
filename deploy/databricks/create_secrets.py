import os
import requests
import logging
import json
import dotenv

def main():
    """ 
    Creates necessary scopes and secrets in Databricks workspace
    """
    logger = logging.getLogger(__name__)

    # Retrieve env variables
    dbi_domain = os.getenv("DBRICKS_DOMAIN")
    token = os.getenv("DBRICKS_TOKEN")

    # Construct api_url
    api_url = "https://" + dbi_domain + "/api/2.0/"
    scope = "storage_scope"

    logger.info("Creating secret..")
    # Blob storage
    create_or_update_secret(api_url, token, scope, secret_name="storage_account", secret_value=os.getenv("BLOB_STORAGE_ACCOUNT"))
    create_or_update_secret(api_url, token, scope, secret_name="storage_key", secret_value=os.getenv("BLOB_STORAGE_KEY"))

    # Eventhubs
    create_or_update_secret(api_url, token, scope, secret_name="eventhub_namespace", secret_value=os.getenv("EVENTHUB_NAMESPACE"))
    create_or_update_secret(api_url, token, scope, secret_name="eventhub_ratings", secret_value=os.getenv("EVENTHUB_RATINGS"))
    create_or_update_secret(api_url, token, scope, secret_name="eventhub_ratings_key", secret_value=os.getenv("EVENTHUB_RATINGS_KEY"))

def create_or_update_secret(api_url, token, scope, secret_name, secret_value):
    """ 
    Creates or updates secrets in Databricks workspace. 
    It will create or reuse existing scopes in the workspace and will overwrite existing secrets.
    """
    if not is_scope_exists(api_url, token, scope):
        create_scope(api_url, token, scope)
    create_secret(api_url, token, scope, secret_name, secret_value)


def is_scope_exists(api_url, token, scope):
    """ 
    Checks if scope exists in Databricks workspace
    """
    scopes = list_scope(api_url, token)
    if not scopes:
        return False
    for s in scopes["scopes"]:
        if scope == s["name"]:
            return True
    return False


def list_scope(api_url, token):
    """ 
    Lists scopes in Databricks workspace
    """
    r = requests.get(api_url + 'preview/secret/scopes/list',
        headers={"Authorization" : "Bearer " + token})
    response_body = r.json()
    if r.status_code != 200:
        raise Exception('Error creating scope: ' + json.dumps(response_body))
    return(response_body)


def create_scope(api_url, token, scope):
    """ 
    Creates a scope in Databricks workspace.
    This will fail if scope already exists.
    """
    r = requests.post(api_url + 'preview/secret/scopes/create',
        headers={"Authorization" : "Bearer " + token},
        json={"scope": scope})
    response_body = r.json()
    if r.status_code != 200:
        raise Exception('Error creating scope: ' + json.dumps(response_body))
    return(response_body)


def create_secret(api_url, token, scope, secret_name, secret_value):
    """ 
    Creates a secret in Databricks workspace in the given scope.
    This will overwrite any existing secrets with the same name.
    """
    r = requests.post(api_url + 'preview/secret/secrets/write',
        headers={"Authorization" : "Bearer " + token},
        json={"scope": scope, "key": secret_name, "string_value": secret_value})
    response_body = r.json()
    if r.status_code != 200:
        raise Exception('Error creating scope: ' + json.dumps(response_body)) 
    return(response_body)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # Get project directory
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # Load dotenv
    dotenv_path = os.path.join(project_dir, '.env')
    dotenv.load_dotenv(dotenv_path)

    main()