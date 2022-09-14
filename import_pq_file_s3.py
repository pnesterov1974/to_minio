from datetime import datetime

import pyarrow.parquet as pq
from pyarrow import fs

from records import Records
from setup_source import SetupSource
from lfs import LFS
from import_pq_schema import ImportPqSchema
from settings import ENDPOINT, ACCESS_KEY, SECRET_KEY, DWH_BUCKET


class ImportPqFileS3:
    minio_endpoint = ENDPOINT
    minio_access_key = ACCESS_KEY
    minio_secret_key = SECRET_KEY
    minio_dwh_bucket = DWH_BUCKET
    batch_record_count = None

    def __init__(self, setup: SetupSource, lfs: LFS, pqschema: ImportPqSchema, big_file=False):
        self.__sql = setup.source_sql
        self.__job_queue_runpack_id = setup.job_queue_runpack_id
        self.__pqschema = pqschema
        self.__destination_info = lfs.lfs_info
        self.__big_file = big_file

    def __do_import_regular_file(self):
        dest_name = self.__destination_info['short_dest_name']
        dest_file = f'{ImportPqFileS3.minio_dwh_bucket}/{dest_name}'
        #dest_file = 'test-1/file.name'
        print(f'Destination file: {dest_file}')
        pqw = None
        import_is_ok = False

        fs3 = fs.S3FileSystem(
            endpoint_override=ImportPqFileS3.minio_endpoint,
            access_key=ImportPqFileS3.minio_access_key,
            secret_key=ImportPqFileS3.minio_secret_key,
            scheme="http",
            )

        #records = Records(self.__sql)
        records = Records(sql=self.__sql, job_queue_runpack_id=self.__job_queue_runpack_id)
        print(f'Старт считывания данных...')
        try:
            for i, r in enumerate(records, 1):
                self.__pqschema.append_record_to_list(r)
            t = self.__pqschema.make_table()
            pqw = pq.ParquetWriter(
                dest_file, t.schema,
                filesystem=fs3,
                )
            pqw.write_table(t)
            
            print(f'Импортировано {i} записей')
            import_is_ok = True
        except Exception as Ex:
            print('Ex=', '\n', str(Ex))
            raise
        finally:
            if pqw:
                pqw.close()
                print(f'Файл {dest_file} закрыт')
            if import_is_ok:
                print('Создание файла .parquet ок')
            return import_is_ok

    def __do_import_big_file(self):
        assert ImportPqFileS3.batch_record_count > 0, 'Не задан размер пачки'
        dest_name = self.__destination_info['short_dest_name']
        dest_file = f'{ImportPqFileS3.minio_dwh_bucket}/{dest_name}'
        #dest_file = 'test-1/file.name'
        print(f'Destination file: {dest_file}')
        pqw = None
        import_is_ok = False

        fs3 = fs.S3FileSystem(
            endpoint_override=ImportPqFileS3.minio_endpoint,
            access_key=ImportPqFileS3.minio_access_key,
            secret_key=ImportPqFileS3.minio_secret_key,
            scheme="http",
            )

        records = Records(self.__sql)
        print(f'Старт считывания данных...')
        current_batch = 1
        try:
            for i, r in enumerate(records, 1):
                if (i % ImportPqFileS3.batch_record_count) == 0:
                    t = self.__pqschema.make_table()
                    if current_batch==1:
                        pqw = pq.ParquetWriter(
                            dest_file, t.schema,
                            filesystem=fs3,
                        )
                    pqw.write_table(t)
                    print(f'пачка {current_batch}, записей {i}')
                    current_batch += 1
                    self.__pqschema.clear_lists()
                self.__pqschema.append_record_to_list(r)
            
            if not self.__pqschema.schema_is_clean():
                t = self.__pqschema.make_table()
                if current_batch==1:
                    pqw = pq.ParquetWriter(
                        dest_file, t.schema,
                        filesystem=fs3,
                    )
                pqw.write_table(t)
                self.__pqschema.clear_lists()
                print(f'пачка {current_batch}, записей {i}')
                import_is_ok = True
        except Exception as Ex:
            print('Ex=', '\n', str(Ex))
            raise
        finally:
            if pqw:
                pqw.close()
                print(f'Файл minio.{dest_file} закрыт')
            if import_is_ok:
                print('Создание файла .parquet ок')
            return import_is_ok

    def __call__(self):
        if self.__big_file:
            self.__do_import_big_file()
        else:
            self.__do_import_regular_file()

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass