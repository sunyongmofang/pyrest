from flask_restful import Resource, request

from .AssembleSql import AssembleSql
from .DatabaseOperate import DatabaseOperate


class NativeSelect(Resource):
    def __init__(self):
        super().__init__()
        self.dbo = DatabaseOperate()
        self.asm = AssembleSql()

    def get(self, DATABASE_, SCHEMA_, TABLE_):
        print(123456)
        # params = request.values
        params = {}
        sqlStr, whereMap = self.asm.getMethod(DATABASE_, SCHEMA_, TABLE_, params)
        res = self.dbo.execute(sqlStr, whereMap)
        return res
