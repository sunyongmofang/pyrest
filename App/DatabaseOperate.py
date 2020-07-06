import pandas
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table

from .Config import Config
from .Logging import Logging


class DatabaseOperate(object):
    engine = None

    def __init__(self):
        if not DatabaseOperate.engine:
            dbUrl = Config.config.database.URL
            DatabaseOperate.engine = create_engine(
                dbUrl,
                pool_size=20,
                max_overflow=0,
                echo=Config.config.debug
            )

    def query(self, sqlObj):
        res = None
        connect = None
        try:
            connect = DatabaseOperate.engine.connect()
            res = pandas.read_sql(sqlObj, connect).to_dict(orient='records')
        except Exception as e:
            Logging.debuglog(e)
        finally:
            if connect:
                connect.close()
        return res

    def execute(self, sqlObject, params=None):
        res = None
        connect = None
        try:
            connect = DatabaseOperate.engine.connect()
            res = connect.execute(sqlObject, params=params).rowcount
        except Exception as e:
            Logging.debuglog(e)
        finally:
            if connect:
                connect.close()
        return { 'a_n': res}
