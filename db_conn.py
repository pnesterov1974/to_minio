
class DbCredentials:

    def __init__(self,
        db_server: str, db_user: str,  db_password: str, db_database: str
        ):
        assert len(db_server) > 0, 'Пустое имя сервера'
        assert len(db_user) > 0, 'Пустое имя пользователя'
        assert len(db_password) > 0, 'Пустой пароль'
        assert len(db_database) > 0, 'Пустое имя базы данных'

        self.__db_server = db_server
        self.__db_user = db_user
        self.__db_password = db_password
        self.__db_database = db_database

    db_sever = property(lambda self: self.__db_server)
    db_user = property(lambda self: self.__db_user)
    db_password = property(lambda self: self.__db_password)
    db_database = property(lambda self: self.__db_database)

    @property
    def credetial_is_valid(self):
        return (len(self.__db_server) > 0)       \
               and (len(self.__db_user) > 0)     \
               and (len(self.__db_password) > 0) \
               and (len(self.__db_database) > 0)

    @property
    def sqla_connection_string(self):
        if self.credetial_is_valid:
            return f'mssql+pymssql://{self.__db_user}:{self.__db_password}@{self.__db_server}/{self.__db_database}'
        # else:
        #     err_mes = 'Не хватает данных для составления строки подключения'
        #     err_add = f'db_server:{self.__db_server} '
        #               f'db_user:{self.__db_user}'
        #               f'db_password:{self.__db_password}'
        #               f'db_database:{self.__db_database}'
        #     raise ValueError('Не хватает данных для составления строки подключения')