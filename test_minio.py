from minio import Minio

from settings import ENDPOINT, ACCESS_KEY, SECRET_KEY, DWH_BUCKET

def copy_file_to_minio():
    
    client = Minio(
         endpoint=ENDPOINT,
         access_key=ACCESS_KEY,
         secret_key=SECRET_KEY,
         secure=False
    )
    
    buckets = client.list_buckets()
    for bucket in buckets:
        print(bucket.name, bucket.creation_date)

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': copy_file_to_minio()