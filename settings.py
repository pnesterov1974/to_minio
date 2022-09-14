import os, os.path

# DB_USER     = r'MERLION\srv_san'
# DB_PASSWORD = r'N32bzwtqLe'
# DB_NAME = r'DB_DWH'
# SERVER_NAME = r'DWH'

DB_USER     = r'sa'
DB_PASSWORD = r'Extlvo12345'
DB_NAME = r'pWORK'
SERVER_NAME = r'192.168.1.78'

#MSSQL_ENGINE_STR = r'mssql+pymssql://sa:Extlvo12345@192.168.1.78/pWORK'
#ENGINE_STR = f'mssql+pymssql://{DB_USER}:{DB_PASSWORD}@{SERVER_NAME}/{DB_NAME}'

BATCH_RECORD_COUNT = 100000
DEFAULT_BATCH_RECORD_COUNT = 1000000
MINIMUM_BATCH_RECORD_COUNT = 1000000

FILE_BASENAME = 'test'
FILE_EXT = 'parquet'

DATA_DIR = os.path.join(os.getcwd(), 'DATA')
FILE_NAME = '.'.join([FILE_BASENAME, FILE_EXT])
FILE_PATH = os.path.join(DATA_DIR, FILE_NAME)

ENDPOINT = '192.168.1.79:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
DWH_BUCKET = 'test-1'

# ENDPOINT = '10.7.5.139'
# ACCESS_KEY = 'dwhuser'
# SECRET_KEY = 'M7K7qc4LLVepp2ab'
# DWH_BUCKET = 'dwh'

if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR)
