from pprint import pprint

from sqlalchemy import create_engine, text

from shared import square_oname

class SetupSource:
    db_credentials = None
    dwh_engine = None

    @classmethod
    def init_setup(cls, db_credentials):
        cls.db_credentials = db_credentials
        cls.__init_db_engine()

    @classmethod
    def __init_db_engine(cls):
        cls.dwh_engine = create_engine(
                cls.db_credentials.sqla_connection_string, 
                echo=False, #isolation_level='READ COMMITTED'
        )

    @staticmethod
    def __get_values_from_setup_table(setup_table_id: int):
        _sql = f'''
            SELECT [dst_table_id],
                   [source_id],
                   CAST([src_schema] AS NVARCHAR(100)) AS [src_schema],
                   CAST([src_table] AS NVARCHAR(100)) AS [src_table],
                   CAST([src_column_timestamp] AS NVARCHAR(100)) AS [src_column_timestamp]
            FROM [DB_DWH].[dbo].[Setup_Tables]
            WHERE [table_id] = {setup_table_id}
            '''
        with SetupSource.dwh_engine.connect() as conn:
            res = conn.execute(text(_sql))
            if res:
                return dict(res.first()._mapping) 
            else:
                raise ValueError(
                    f'для {setup_table_id=} отсутствуют записи в таблице [DB_DWH].[dbo].[Setup_Tables]'
                )

    @staticmethod
    def __get_values_from_setup_table_global(setup_global_table_id: int):
        _sql = f'''
            SELECT CAST([dst_table] AS NVARCHAR(100)) AS [dst_table]
            FROM [DB_DWH].[dbo].[Setup_Tables_Global]
            WHERE [dst_table_id] = {setup_global_table_id}
            '''
        with SetupSource.dwh_engine.connect() as conn:
            res = conn.execute(text(_sql))
            if res:
                return dict(res.first()._mapping)
            else:
               raise ValueError(
                    f'для {setup_global_table_id=} отсутствуют записи в таблице [DB_DWH].[dbo].[Setup_Tables_Global]'
                )
    
    @staticmethod
    def __get_values_from_setup_source(setup_source_id: int):
        _sql = f'''
            SELECT [server_link],  
                   CAST([src_database] AS NVARCHAR(100)) AS [src_database]
            FROM [DB_DWH].[dbo].[Setup_Source]
            WHERE [source_id] = {setup_source_id}
            '''
        with SetupSource.dwh_engine.connect() as conn:
            res = conn.execute(text(_sql))
            if res:
                return dict(res.first()._mapping)
            else:
                raise ValueError(
                    f'для {setup_source_id=} отсутствуют записи в таблице [DB_DWH].[dbo].[Setup_Source]'
                )

    @staticmethod
    def __get_values_from_job_queue(job_id: int):
        _sql = f'''
            SELECT [DWH_Table_ID],
                   [Run_Pack_ID]
            FROM [DB_DWH].[dbo].[Setup_Job_Queue]
            WHERE [Job_ID] = {job_id}
            '''
        with SetupSource.dwh_engine.connect() as conn:
            res = conn.execute(text(_sql))
            if res:
                return dict(res.first()._mapping)
            else:
                raise ValueError(
                    f'для {job_id=} отсутствуют записи в таблице [DB_DWH].[dbo].[Setup_Source]'
                )

    @staticmethod
    def square_fullname(fullname: str):
        s = fullname.split('.')
        return '.'.join(list(map(square_oname, s)))

    def __init__(self, **kwargs):
        self.__source_table_id = None
        self.__job_id = None
        self.__oname = None
        self.__source_schema = None
        self.__source_table_name = None
        self.__source_global_table_id = None
        self.__dest_name = None
        self.__source_id = None
        self.__source_database = None
        self.__source_sql = None
        #self.__columns_info = None
        #self.__columns_names_list_for_select_stmt = None
        self.__use_setup_tables = None
        self.__src_column_timestamp = None
        self.__job_queue_runpack_id = None

        if 'job_id' in kwargs:
            self.__job_id = kwargs['job_id']
            self.__use_setup_tables = True
        if 'source_table_id' in kwargs:
            self.__source_table_id = kwargs['source_table_id']
            self.__get_source_table_info()
            self.__use_setup_tables = True
        elif 'oname' in kwargs:
            self.__oname = SetupSource.square_fullname(kwargs['oname'])
            self.__use_setup_tables = False

        assert self.__use_setup_tables is not None, \
        'Неправильная комбинация параметров. Должен быть или setup_table_id или oname'
        
    def __get_source_table_info(self):
        try:
            if self.__job_id:
                d0 = SetupSource.__get_values_from_job_queue(self.__job_id)
                self.__source_table_id = d0['DWH_Table_ID']
                self.__job_queue_runpack_id = d0['Run_Pack_ID']
                if not self.__job_queue_runpack_id:
                    self.__job_queue_runpack_id = -1
            else:
                self.__job_queue_runpack_id = -1
                
            if self.__source_table_id:
                d1 = SetupSource.__get_values_from_setup_table(self.__source_table_id)
                self.__source_schema = d1['src_schema']
                self.__source_table_name = d1['src_table']
                self.__source_global_table_id = d1['dst_table_id']
                self.__source_id = d1['source_id']
                self.__src_column_timestamp = d1['src_column_timestamp']

                d2 = SetupSource.__get_values_from_setup_table_global(self.__source_global_table_id)
                self.__dest_name = d2['dst_table']

                d3 = SetupSource.__get_values_from_setup_source(self.__source_id)
                self.__source_database = d3['src_database']
        except ValueError as vex:
            print(vex)
            self.__source_schema = None
            self.__source_table_name = None
            self.__source_global_table_id = None
            self.__dest_name = None
            self.__source_id = None
            self.__source_database = None
            self.__src_column_timestamp = None
            self.__job_queue_runpack_id = None
        except Exception as ex:
            raise

    dest_name = property(lambda self: self.__dest_name)
    src_column_timestamp = property(lambda self: self.__src_column_timestamp)
    job_queue_runpack_id = property(lambda self: self.__job_queue_runpack_id)

    @property
    def source_sql(self):
        if not self.__source_sql:
            self.__source_sql = self.__get_source_sql()
            print('==== SQL на источник ==\n')
            print(self.__source_sql)
            print('\n#################')
        return self.__source_sql
            
    def __get_source_sql(self):
        s1 = 'EXEC'
        if self.__use_setup_tables:
            s2 = 'x'
        else:
            s2 = self.__oname 
        return ' '.join([s1, s2])
       

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass