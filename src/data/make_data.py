
import zipfile
import os
import logging
import dotenv
from urllib.request import urlretrieve        
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings

movieset_data_url = "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
blob_out_dir = 'ml-latest-small'
container = 'data'

def main(project_dir):
    """ Downloads data and uploads to blob storage
    """
    logger = logging.getLogger(__name__)

    # Construct paths
    raw_data_dir = os.path.join(project_dir, 'data', 'raw')
    zip_file = os.path.join(raw_data_dir, "data.zip")

    # Download
    logger.info("Downloading data from %(url)s into %(location)s'" % {"url": movieset_data_url, "location": zip_file})
    urlretrieve(movieset_data_url, zip_file)

    # Unzip
    zip_ref = zipfile.ZipFile(zip_file, 'r')
    zip_ref.extractall(raw_data_dir)
    zip_ref.close()

    blob_storage_account = os.getenv("BLOB_STORAGE_ACCOUNT")
    blob_storage_key = os.getenv("BLOB_STORAGE_KEY")
    
    uploadFolderCsvToBlob(blob_storage_account, blob_storage_key, container, raw_data_dir, blob_out_dir)


def uploadFolderCsvToBlob(storage_account, storage_key, container, in_dir, blob_out_dir):
    """ Uploads csvs in a folder into blob storage
    """
    # Create blob service
    block_blob_service = BlockBlobService(account_name=storage_account, account_key=storage_key)

    # Create container if not exists
    if not any(container in c.name for c in block_blob_service.list_containers()):
        block_blob_service.create_container(container, fail_on_exist=False)

    # Upload the CSV file to Azure cloud
    for root, dirs, filenames in os.walk(in_dir):
        for filename in filenames:
            if os.path.splitext(filename)[-1] == ".csv":
                blob_name = blob_out_dir + '/' + filename
                file_path = os.path.join(root, filename)
                # Upload the CSV file to Azure cloud
                block_blob_service.create_blob_from_path(
                    container, blob_name, file_path,
                    content_settings=ContentSettings(content_type='application/CSV'))


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # Get project directory
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # Load dotenv
    dotenv_path = os.path.join(project_dir, '.env')
    dotenv.load_dotenv(dotenv_path)
    
    # Run
    main(project_dir)