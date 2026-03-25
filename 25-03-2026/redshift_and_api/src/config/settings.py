import os

from dotenv import load_dotenv

load_dotenv()

WORK_GROUP_NAME = os.getenv('WORK_GROUP_NAME')
DATABASE = os.getenv('DATABASE')