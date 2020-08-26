import pandas
import json
from sqlalchemy import create_engine
from sqlalchemy import text

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

    def query(self, sqlObj: text):
        res = None
        connect = None
        try:
            connect = DatabaseOperate.engine.connect()
            res = pandas.read_sql(sqlObj, connect).to_json(orient='records')
            res = json.loads(res)
        except Exception as e:
            Logging.debuglog(e)
        finally:
            if connect:
                connect.close()
        return res

    def execute(self, sqlObject: text, params: dict = None):
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
        return { 'a_n': res }
