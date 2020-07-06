from flask_restful import Resource, request

from .AssembleSql import AssembleSql
from .DatabaseOperate import DatabaseOperate


class NativeSelect(Resource):
    def __init__(self):
        super().__init__()
        self.asm = AssembleSql()

    def get(self, DATABASE_, SCHEMA_, TABLE_):
        params = request.values
        res = self.asm.getMethod(SCHEMA_, TABLE_, params)
        return res

    def post(self, DATABASE_, SCHEMA_, TABLE_):
        data = request.json
        res = self.asm.postMethod(SCHEMA_, TABLE_, data)
        return res

    def put(self, DATABASE_, SCHEMA_, TABLE_):
        params = request.values
        data = request.json
        res = self.asm.putMethod(SCHEMA_, TABLE_, params, data)
        return res

    def patch(self, DATABASE_, SCHEMA_, TABLE_):
        params = request.values
        data = request.json
        res = self.asm.putMethod(SCHEMA_, TABLE_, params, data)
        return res

    def delete(self, DATABASE_, SCHEMA_, TABLE_):
        params = request.values
        res = self.asm.delMethod(SCHEMA_, TABLE_, params)
        return res
