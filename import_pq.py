from shared import ImportAs 
from import_pq_schema import ImportPqSchema
from import_pq_file_lfs import ImportPqFileLfs
from import_pq_file_s3 import ImportPqFileS3
from import_pq_folder_lfs import ImportPqFolderLfs
from import_pq_folder_s3 import ImportPqFolderS3
from setup_source import SetupSource
from lfs import LFS

def do_import(jid: int, sid: int, import_as: ImportAs, direct_to_s3=False,
              where_str=None, partition_field=None
             ):
    sql_where_str = where_str if where_str else 'WHERE 0=0'
    if sid == -1:
        ss = SetupSource(oname='pWORK.POLISHED.Polished', import_as=import_as, sql_where_str=sql_where_str)
        lfs = LFS(dest_name='AT_HOME', import_as=import_as)
        partition_fields = ['InDocID', 'RndFncID']
    elif sid >= 0:
        ss = SetupSource(job_id=jid, source_table_id=sid, sql_where_str=sql_where_str)
        lfs = LFS(dest_name=ss.dest_name, import_as=import_as)
        #partition_fields = [partition_field] if partition_field else None
        partition_fields=None
        print(f'partition_fields=')
        
    ips = ImportPqSchema(ss)

    if import_as == ImportAs.file:
        if direct_to_s3:
            _imp = ImportPqFileS3(setup=ss, lfs=lfs, pqschema=ips, big_file=False)
            _imp()
        else:
            _imp = ImportPqFileLfs(setup=ss, lfs=lfs, pqschema=ips, big_file=False)
            _imp(copy_to_minio=True, delete_file_after_copy=False)
    elif import_as == ImportAs.big_file:
        if direct_to_s3:
            _imp = ImportPqFileS3(setup=ss, lfs=lfs, pqschema=ips, big_file=True)
            _imp()
        else:
            _imp = ImportPqFileLfs(setup=ss, lfs=lfs, pqschema=ips, big_file=True)
            _imp(copy_to_minio=True, delete_file_after_copy=False)
    elif import_as == ImportAs.folder:
        if direct_to_s3:
            _imp = ImportPqFolderS3(setup=ss, lfs=lfs, pqschema=ips, partition_fields=partition_fields)
            _imp()
        else:
            _imp = ImportPqFolderLfs(setup=ss, lfs=lfs, pqschema=ips, partition_fields=partition_fields)
            _imp(copy_to_minio=True, delete_folder_after_copy=False)

    return lfs #if f else None
        
# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass