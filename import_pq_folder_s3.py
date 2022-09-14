from datetime import datetime

import pyarrow.parquet as pq
from pyarrow import fs

from import_pq_schema import ImportPqSchema
from setup_source import SetupSource
from lfs import LFS
from records import Records
from settings import ENDPOINT, ACCESS_KEY, SECRET_KEY, DWH_BUCKET, DATA_DIR

class ImportPqFolderS3:
    minio_endpoint = ENDPOINT
    minio_access_key = ACCESS_KEY
    minio_secret_key = SECRET_KEY
    minio_dwh_bucket = DWH_BUCKET

    def __init__(self, setup: SetupSource, lfs: LFS, partition_fields: list):
        self.__sql = setup.source_sql
        self.__job_queue_runpack_id = setup.job_queue_runpack_id
        self.__lfs_path = lfs.lfs_info['full_source_name']
        self.__lfs = lfs
        self.__partition_fields = partition_fields

    def __call__(self):
        self.__do_import()

    def __do_import(self):
        _rel_path = self.__lfs_path.replace(DATA_DIR, '').replace('\\', '/')
        #print(f'{_rel_path=}')
        root_path = f'{ImportPqFolderS3.minio_dwh_bucket}{_rel_path}'
        print(f'{root_path=}')
        print(f'{self.__partition_fields=}')
        
        fs3 = fs.S3FileSystem(
            endpoint_override=ImportPqFolderS3.minio_endpoint,
            access_key=ImportPqFolderS3.minio_access_key,
            secret_key=ImportPqFolderS3.minio_secret_key,
            scheme="http",
            )
        import_is_ok = False
        records = Records(sql=self.__sql, job_queue_runpack_id=self.__job_queue_runpack_id)
        ips = ImportPqSchema()
        try:
            for i, r in enumerate(records, 1):
                if i==1:
                    ips.init_columns(r)
                ips.append_record_to_list(r)
            t = ips.make_table()
            
            #TODO Common_metadata from lfs
            pq.write_to_dataset(t, 
                    root_path = root_path, 
                    partition_cols=self.__partition_fields,
                    filesystem=fs3
            )
            import_is_ok = True
        except Exception as Ex:
            print('Ex=', '\n', str(Ex))
            raise
        finally:
            if import_is_ok:
                print('Создание папки из parquet - файлов  ok')
            return import_is_ok

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass
