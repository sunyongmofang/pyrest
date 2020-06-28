from flask import Flask
from flask_restful import Api

from .NativeSelect import NativeSelect


class PyRest(object):

    @staticmethod
    def main():
        pyrest = PyRest()
        pyrest.runRest()

    def runRest(self):
        app = Flask(__name__)
        api = Api(app)
        api.add_resource(NativeSelect, '/<DATABASE_>/<SCHEMA_>/<TABLE_>')
        app.run(debug=True, host='0.0.0.0', port=6000)
