import argparse
from work import work

from shared import ImportAs
from settings import DEFAULT_BATCH_RECORD_COUNT, MINIMUM_BATCH_RECORD_COUNT


#уйти от [] в именах
#то поле, которое будет в partitionFields - в маппинге без пробелов и симоволов %№;...
#добавить 3 поля в выходнрй набов
#   PackID из JobQueue
#   SnapshotDate из имени файла
# lfs - создание через stream file system
# импорт-фунуторы возвращают количество записей


# обработка варианта когда из источника не возвращена ни одна запись
# print sql make nice
# use lfs via enum
# timestamp to S3 проверить !. ts в MSSQL via pytable viw
# after direct import to s3 read metadata

#exceptions -> multi-line error messages
#logging

#name = "Eric"
#profession = "comedian"
#affiliation = "Monty Python"
#message = (
#    f"Hi {name}. "
#    f"You are a {profession}. "
#    f"You were in {affiliation}."
#)

def main():
    parser = argparse.ArgumentParser(description='Importing DWH Data to S3 minio storage')
    parser.add_argument(
        '-sid', '--setup_table_id', dest="setup_table_id", required=False, type=int, default=0
    )
    parser.add_argument(
        '-jid', '--job_id', dest="job_id", required=False, type=int, default=0
    )
    parser.add_argument(
        '-bsz', '--batch_size', dest="batch_size", required=False, type=int,
        default=MINIMUM_BATCH_RECORD_COUNT
    )
    parser.add_argument(
        '-df', '--delete_local_file', dest="delete_local_file", required=False, type=str,
        choices=['delete', 'not_delete'], default='not_delete'
    )
    parser.add_argument(
        '-as', '--import_as', dest="import_as", required=False, type=str, 
        choices=['file', 'folder', 'big_file'], default='folder'
    )
    parser.add_argument(
        '-tg', '--target', dest="target", required=False, type=str, 
        choices=['local', 'S3'], default='S3'
    )
    parser.add_argument(
        '-wh', '--where', dest="where", required=False, type=str, 
        default=None
    )
    parser.add_argument(
        '-pf', '--partition_field', dest="partition_field", required=False, type=str, 
        default=None
    )
    parser.add_argument(
        '-ds', '--dwh_server', dest="dwh_server", required=False, type=str, 
        default=None
    )
    args = parser.parse_args()    
    print('==== Входящие аргументы =')
    print(f'{args.setup_table_id=}')
    print(f'{args.job_id=}')
    print(f'{args.batch_size=}')
    print(f'{args.delete_local_file=}')
    print(f'{args.import_as=}')
    print(f'{args.target=}')
    print(f'{args.where=}')
    print(f'{args.partition_field=}')
    print(f'{args.dwh_server=}')
    print('\n')
    if args.batch_size:
        batch_record_count = args.batch_size
        if batch_record_count < MINIMUM_BATCH_RECORD_COUNT:
            batch_record_count = MINIMUM_BATCH_RECORD_COUNT
    else:
        batch_record_count = DEFAULT_BATCH_RECORD_COUNT

    delete_local_file = (args.delete_local_file == 'delete')
    direct_to_s3 = (args.target =='S3')
    if args.import_as == 'file':
        import_as_choise = ImportAs.file
    elif args.import_as == 'folder':
        import_as_choise = ImportAs.folder
    elif args.import_as == 'big_file':
        import_as_choise = ImportAs.big_file

    work(setup_table_id=args.setup_table_id, 
         batch_record_count=batch_record_count,
         delete_local_file=delete_local_file,
         import_as=import_as_choise,
         job_id=args.job_id,
         direct_to_s3=direct_to_s3,
         where_str = args.where,
         partition_field=args.partition_field,
         dwh_server=args.dwh_server
        )

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': main()