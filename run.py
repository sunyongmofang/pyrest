from flask import Flask
from flask_restful import Api
from flask_restful import Resource
from flask_sqlalchemy import SQLAlchemy


class HelloWorld(Resource):
    def get(self):
        return {'hello': 'world'}


class PyRest(object):

    @staticmethod
    def main():
        pyrest = PyRest()
        pyrest.runRest()

    def runRest(self):
        app = Flask(__name__)
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://master:123456@127.0.0.1:5432/test'
        db = SQLAlchemy(app)
        api = Api(app)
        api.add_resource(HelloWorld, '/')
        app.run(debug=True, host='0.0.0.0', port=6000)


if __name__ == '__main__':
    PyRest.main()
