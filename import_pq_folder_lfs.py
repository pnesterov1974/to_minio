from datetime import datetime

import pyarrow.parquet as pq
from minio import Minio

from import_pq_schema import ImportPqSchema
from setup_source import SetupSource
from lfs import LFS
from records import Records
from settings import ENDPOINT, ACCESS_KEY, SECRET_KEY, DWH_BUCKET

class ImportPqFolderLfs:
    minio_endpoint = ENDPOINT
    minio_access_key = ACCESS_KEY
    minio_secret_key = SECRET_KEY
    minio_dwh_bucket = DWH_BUCKET

    @staticmethod
    def copy_file_to_minio(abs_source_path: str, rel_minio_path: str):
        minio = Minio(
            endpoint=ImportPqFolderLfs.minio_endpoint,
            access_key=ImportPqFolderLfs.minio_access_key,
            secret_key=ImportPqFolderLfs.minio_secret_key,
            secure=False
        )
        minio.fput_object(
                bucket_name=ImportPqFolderLfs.minio_dwh_bucket,
                object_name=rel_minio_path,
                file_path=abs_source_path
            )

    def __init__(self, 
        setup: SetupSource, lfs: LFS, pqschema: ImportPqSchema,
        partition_fields: list
        ):
        self.__sql = setup.source_sql
        self.__job_queue_runpack_id = setup.job_queue_runpack_id
        self.__lfs_path = lfs.lfs_info['full_source_name']
        self.__lfs = lfs
        self.__partition_fields = partition_fields
        self.__pqschema = pqschema

    def __call__(self, copy_to_minio=True, delete_folder_after_copy=False):
        self.__do_import()
        if copy_to_minio:
             self.__copy_folder_to_minio(delete_folder_after_copy)

    def __do_import(self):
        print(f'Destination path: {self.__lfs_path}')
        import_is_ok = False
        #records = Records(self.__source_sql)
        records = Records(sql=self.__sql, job_queue_runpack_id=self.__job_queue_runpack_id)
        print(f'Старт считывания данных...')
        try:
            for i, r in enumerate(records, 1):
                self.__pqschema.append_record_to_list(r)
            t = self.__pqschema.make_table()
            _root_path = self.__lfs_path
            _common_metadata = self.__lfs_path + '/_common_metadata'
            pq.write_to_dataset(t, 
                    root_path = _root_path, 
                    partition_cols=self.__partition_fields,
            )
            pq.write_metadata(t.schema, _common_metadata)
            import_is_ok = True
        except Exception as Ex:
            print('Ex=', '\n', str(Ex))
            raise
        finally:
            if import_is_ok:
                print('Создание папки из parquet - файлов  ok')
            return import_is_ok

    def __copy_folder_to_minio(self, delete_folder_after_copy=False):
        print('копирую папку в minio...')
        copy_ok = False
        try:
            for source_file_fullpath, dest_file_short_path in self.__lfs.walk_lfs():
                ImportPqFolderLfs.copy_file_to_minio(source_file_fullpath, dest_file_short_path)
            copy_ok = True
        except:
            raise
        finally:
            if copy_ok:
                 print('копирование папки в minio завершено ...')
            if copy_ok and delete_folder_after_copy:
                pass
                #TO-DO: delete folder
        

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass
