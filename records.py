from sqlalchemy import create_engine, text

class Records:
    db_credentials = None
    dwh_engine = None
    snapshot_dt = None

    @classmethod
    def init_records(cls, db_credentials, snapshot_dt:str):
        cls.db_credentials = db_credentials
        cls.snapshot_dt = snapshot_dt
        cls.__init_engine_str()
        cls.__init_db_engine()
        
    @classmethod
    def __init_engine_str(cls):
        if cls.db_credentials:
            cls.source_engine_str = cls.db_credentials.sqla_connection_string
        else:
            pass #raise bad credential infp

    @classmethod
    def __init_db_engine(cls):
        if cls.source_engine_str:
            cls.source_engine = create_engine(
                cls.source_engine_str, echo=False, #isolation_level='READ COMMITTED'
        )
        else:
            pass #raise no connection string

    def __init__(self, sql: str, job_queue_runpack_id=None):
        self.__sql = sql
        self.__job_queue_runpack_id = job_queue_runpack_id

    def __iter__(self):
        with Records.source_engine.connect() as conn:
            res = conn.execute(text(self.__sql))
            for row in res:
                d = dict(row._mapping)
                if self.__job_queue_runpack_id:
                    d['RunPackID'] = self.__job_queue_runpack_id
                d['SnapshotDT'] = Records.snapshot_dt
                yield d