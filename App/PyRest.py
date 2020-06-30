from flask import Flask
from flask_restful import Api

from .NativeSelect import NativeSelect
from .Config import Config


class PyRest(object):

    @staticmethod
    def main(configPath):
        Config(configPath)
        PyRest().runRest()

    def runRest(self):
        app = Flask(__name__)
        api = Api(app)
        api.add_resource(NativeSelect, '/<DATABASE_>/<SCHEMA_>/<TABLE_>')
        app.run(debug=True, host='0.0.0.0', port=Config.config.http.port)
