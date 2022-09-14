from datetime import datetime

from shared import ImportAs, get_filename_timestamp
from settings import DB_USER, DB_PASSWORD, DB_NAME, SERVER_NAME
from db_conn import DbCredentials
from setup_source import SetupSource
from records import Records
from import_pq_schema import ImportPqSchema
from import_pq_file_lfs import ImportPqFileLfs
from import_pq_file_s3 import ImportPqFileS3
from import_pq import do_import
from lfs import LFS

def work(setup_table_id: int,
        batch_record_count: int,
        job_id=0,
        delete_local_file=False, 
        import_as=ImportAs.file,
        direct_to_s3=False,
        where_str=None,
        partition_field=None,
        dwh_server=None
    ):

    ImportPqFileLfs.batch_record_count = batch_record_count
    ImportPqFileS3.batch_record_count = batch_record_count

    source_server = dwh_server if dwh_server else SERVER_NAME
    source_dbc = DbCredentials(db_server=source_server, db_database=DB_NAME,
        db_user=DB_USER, db_password=DB_PASSWORD
        )
    SetupSource.init_setup(db_credentials=source_dbc)
    
    records_dbc = DbCredentials(db_server=SERVER_NAME, db_database=DB_NAME,
        db_user=DB_USER, db_password=DB_PASSWORD
        )
    # -------- основной snapshot_dt
    snapshot_dt = get_filename_timestamp(datetime.now())
    # --------
    ImportPqSchema.snapshot_dt = snapshot_dt
    LFS.snapshot_dt = snapshot_dt

    Records.init_records(db_credentials=records_dbc, snapshot_dt=snapshot_dt)

    dst = do_import(
        jid=job_id, sid=setup_table_id, import_as=import_as, direct_to_s3=direct_to_s3,
        where_str=where_str, partition_field=partition_field
        )
    
# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass