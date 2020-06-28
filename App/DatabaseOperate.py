import pandas
from sqlalchemy import create_engine


class DatabaseOperate(object):
    engine = None

    def __init__(self):
        if not DatabaseOperate.engine:
            dbUrl = 'postgresql+psycopg2://master:123456@127.0.0.1:5432/test'
            DatabaseOperate.engine = create_engine(
                dbUrl,
                pool_size=20,
                max_overflow=0
            )

    def execute(self, sql, whereMap={}):
        res = None
        connect = None
        try:
            connect = DatabaseOperate.engine.connect()
            res = pandas.read_sql(
                sql, connect, params=whereMap).to_dict(orient='records')
        except Exception as e:
            print(e)
        finally:
            if connect:
                connect.close()
        return res
