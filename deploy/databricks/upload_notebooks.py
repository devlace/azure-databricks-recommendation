import os
import requests
import logging
import json
import dotenv
from subprocess import Popen, PIPE

def main(project_dir):
    """ 
    Creates necessary scopes and secrets in Databricks workspace
    """
    logger = logging.getLogger(__name__)

    # Get notebook directory
    notebooks_dir = os.path.join(project_dir, "notebooks")

    logger.info("importing directory: " + notebooks_dir)
    Popen(["databricks", "workspace", "import_dir", notebooks_dir, "/shared_notebooks"]).wait()

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # Get project directory
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir)

    # Load dotenv
    dotenv_path = os.path.join(project_dir, '.env')
    dotenv.load_dotenv(dotenv_path)

    main(project_dir)