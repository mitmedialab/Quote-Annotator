import os
import logging
import pymongo
from dotenv import load_dotenv

load_dotenv()  # load config from .env file

# set up logging
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger(__name__)
logger.info("------------------------------------------------------------------------")
logger.info("Starting up Quote Worker")

DB_NAME = os.environ['DB_NAME']
logger.info("DB_NAME: {}".format(DB_NAME))

COLLECTION_NAME = os.environ['COLLECTION_NAME']
logger.info("COLLECTION_NAME: {}".format(COLLECTION_NAME))

BROKER_URL = os.environ['BROKER_URL']
logger.info("BROKER_URL: {}".format(BROKER_URL))

NLP_URL = os.environ['NLP_URL']
logger.info("NLP_URL: {}".format(NLP_URL))

MONGO_DSN = os.environ['MONGO_DSN']
logger.info("MONGO_DSN: {}".format(MONGO_DSN))


# factory method to a connection to the mongo db full of stories
def get_db_client():
    client = pymongo.MongoClient(MONGO_DSN)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return collection
