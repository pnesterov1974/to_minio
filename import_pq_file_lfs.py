import os
from datetime import datetime
from pprint import pprint

import pyarrow.parquet as pq
from minio import Minio

from records import Records
from setup_source import SetupSource
from lfs import LFS
from import_pq_schema import ImportPqSchema
from settings import ENDPOINT, ACCESS_KEY, SECRET_KEY, DWH_BUCKET


class ImportPqFileLfs:
    minio_endpoint = ENDPOINT
    minio_access_key = ACCESS_KEY
    minio_secret_key = SECRET_KEY
    minio_dwh_bucket = DWH_BUCKET
    batch_record_count = None

    @staticmethod
    def read_pq_info(file_path: str):
        if os.path.exists(file_path):
            print(f'Считываю метаданные {file_path}')
            pq_md = pq.read_metadata(file_path)
            print(pq_md)

    @staticmethod
    def copy_file_to_minio(abs_source_path: str, rel_minio_path: str):
        print('копирую файл в minio...')
        try:
            minio = Minio(
                endpoint=ImportPqFileLfs.minio_endpoint,
                access_key=ImportPqFileLfs.minio_access_key,
                secret_key=ImportPqFileLfs.minio_secret_key,
                secure=False
            )
            minio.fput_object(
                    bucket_name=ImportPqFileLfs.minio_dwh_bucket,
                    object_name=rel_minio_path,
                    file_path=abs_source_path
                )
            print('копирование файла в minio завершено...')
        except:
            raise

    def __init__(self, 
        setup: SetupSource, lfs: LFS, pqschema: ImportPqSchema, 
        big_file=False, 
        ):
        self.__sql = setup.source_sql
        self.__job_queue_runpack_id = setup.job_queue_runpack_id
        #print(self.__job_queue_runpack_id)
        self.__pqschema = pqschema
        self.__destination_info = lfs.lfs_info
        self.__big_file = big_file

    def __do_import_big_file(self):
        assert ImportPqFileLfs.batch_record_count > 0, 'Не задан размер пачки'
        lfs_dest_filepath = self.__destination_info['full_source_name']
        print(f'Destination file path: {lfs_dest_filepath}')
        pqw = None
        import_is_ok = False
        records = Records(sql=self.__sql, job_queue_runpack_id=self.__job_queue_runpack_id)
        current_batch = 1
        print(f'Старт считывания данных...')
        try:
            for i, r in enumerate(records, 1):
                if (i % ImportPqFileLfs.batch_record_count) == 0:
                    t = self.__pqschema.make_table()
                    if current_batch==1:
                        pqw = pq.ParquetWriter(lfs_dest_filepath, t.schema)
                    pqw.write_table(t)
                    #print(f'пачка {current_batch}, записей {i}, время создания пачки {dt_delta}')
                    print(f'пачка {current_batch}, записей {i}')
                    current_batch += 1
                    self.__pqschema.clear_lists()
                self.__pqschema.append_record_to_list(r)
            
            if not self.__pqschema.schema_is_clean():
                t = self.__pqschema.make_table()
                if current_batch==1:
                    pqw = pq.ParquetWriter(lfs_dest_filepath, t.schema)
                pqw.write_table(t)
                dt = datetime.now()
                self.__pqschema.clear_lists()
                #print(f'пачка {current_batch}, записей {i}, время создания пачки {dt_delta}')
                print(f'пачка {current_batch}, записей {i}')
            import_is_ok = True
        except Exception as Ex:
            print('Ex=', '\n', str(Ex))
            raise
        finally:
            if pqw:
                pqw.close()
                print(f'Файл {lfs_dest_filepath} закрыт')
            if import_is_ok:
                print('Создание файла .parquet ок')
                #print(f'Полное время создания файла :{full_dt}')
            return import_is_ok

    def __do_import_regular_file(self):
        lfs_dest_filepath = self.__destination_info['full_source_name']
        print(f'Destination file path: {lfs_dest_filepath}')
        pqw = None
        import_is_ok = False
        records = Records(sql=self.__sql, job_queue_runpack_id=self.__job_queue_runpack_id)
        print(f'Старт считывания данных...')
        try:    
            for i, r in enumerate(records, 1):
                self.__pqschema.append_record_to_list(r)
            t = self.__pqschema.make_table()
            pqw = pq.ParquetWriter(lfs_dest_filepath, t.schema)
            pqw.write_table(t)
            print(f'Импортировано {i} записей')
            import_is_ok = True
        except Exception as Ex:
            print('Ex=', '\n', str(Ex))
            raise
        finally:
            if pqw:
                pqw.close()
                print(f'Файл {lfs_dest_filepath} закрыт')
            if import_is_ok:
                print('Создание файла .parquet ок')
            return import_is_ok

    def __call__(self, copy_to_minio=True, delete_file_after_copy=False):
        if self.__big_file:
            if self.__do_import_big_file():
                ImportPqFileLfs.read_pq_info(self.__destination_info['full_source_name'])
                if copy_to_minio:
                    self.__copy_file_to_minio(delete_file_after_copy)
        else:
            if self.__do_import_regular_file():
                ImportPqFileLfs.read_pq_info(self.__destination_info['full_source_name'])
                if copy_to_minio:
                    self.__copy_file_to_minio(delete_file_after_copy)
            else:
                print('Файл .parquet не создан')
        
    def __copy_file_to_minio(self, delete_file_after_copy: False):
        abs_source_path = self.__destination_info['full_source_name']
        short_dest_name = self.__destination_info['short_dest_name']
        ImportPqFileLfs.copy_file_to_minio(
            abs_source_path=abs_source_path, rel_minio_path=short_dest_name
            )
        if delete_file_after_copy:
            os.remove(abs_source_path)
            if not os.path.exists(abs_source_path):
                print(f'{abs_source_path} удален')

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass