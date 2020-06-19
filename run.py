from flask import Flask
from flask_restful import Api, Resource


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
        api = Api(app)
        api.add_resource(HelloWorld, '/')
        app.run(debug=True, host='0.0.0.0', port=6000)


if __name__ == '__main__':
    PyRest.main()
