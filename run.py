from flask import Flask
from flask_restful import Api
from flask_restful import Resource
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine


class HelloWorld(Resource):
    def get(self):
        return {'hello': 'world'}


class DatabaseOperate(object):
    engine = None

    def __init__(self):
        if not DatabaseOperate.engine:
            DatabaseOperate.engine = create_engine('postgresql+psycopg2://master:123456@127.0.0.1:5432/test', pool_size=20, max_overflow=0)

    def execute(self, sql):
        res = None
        try:
            connect = DatabaseOperate.engine.connect()
            res = connect.execute(sql)
        except Exception as e:
            print(e)
        finally:
            if connect:
                connect.close()
        return res


class PyRest(object):

    @staticmethod
    def main():
        pyrest = PyRest()
        pyrest.runRest()

    def runRest(self):
        app = Flask(__name__)
        dbo = DatabaseOperate()
        res = dbo.execute('select * from t_student1')
        # app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://master:123456@127.0.0.1:5432/test'
        # app.config['SQLALCHEMY_ECHO'] = True
        # app.config['SQLALCHEMY_POOL_SIZE'] = 10
        # db = SQLAlchemy(app)
        # engine = db.get_engine()
        # connect = engine.connect()
        # res = connect.execute('select * from t_student')
        # connect.close()
        print(list(res))
        api = Api(app)
        api.add_resource(HelloWorld, '/')
        app.run(debug=True, host='0.0.0.0', port=6000)


if __name__ == '__main__':
    PyRest.main()
