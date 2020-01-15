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

DB_NAME = "mc-quotes2"
COLLECTION_NAME = "define_american"

BROKER_URL = os.environ['BROKER_URL']
logger.info("BROKER_URL: {}".format(BROKER_URL))

NLP_URL = os.environ['NLP_URL']
logger.info("NLP_URL: {}".format(NLP_URL))

MONGO_DSN = os.environ['MONGO_DSN']
logger.info("MONGO_DSN: {}".format(MONGO_DSN))


def get_db_client():
    client = pymongo.MongoClient(MONGO_DSN)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return collection
