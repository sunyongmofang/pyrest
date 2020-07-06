import logging
import operator

from sqlalchemy import text
from sqlalchemy import func
from sqlalchemy import desc
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.sql import select
from sqlalchemy.sql import and_

from .Config import Config
from .Logging import Logging
from .DatabaseOperate import DatabaseOperate


class AssembleSql(object):

    def __init__(self):
        self.keyword = ['_select', '_count', '_page',
                        '_page_size', '_groupby', '_orderby']
        self.keywordSet = set(self.keyword)
        self.dbo = DatabaseOperate()

    def getMethod(self, SCHEMA_: str, TABLE_: str, params):
        def attrgetter(obj, att): return operator.attrgetter(att)(obj)
        tableModel = self.getModel(SCHEMA_, TABLE_)
        values = [params.get(x) for x in self.keyword]
        result = None

        _select = values[0].split(',') if values[0] else None
        _count = [func.count(attrgetter(tableModel.c, values[1])).label(
            'count')] if values[1] else None
        _page = None
        _page_size = None
        _groupby = values[4].split(',') if values[4] else [None]
        _orderby = [x if '-' == x[0] else desc(x[1:])
                    for x in values[5].split(',')] if values[5] else [None]
        if values[2] and values[3]:
            _page = values[2]
            _page_size = values[3]

        _where = [attrgetter(tableModel.c, key) == params.get(key)
                  for key in params if key not in self.keywordSet]

        _fields = None
        if _select or _count:
            _fields = _count if _count else [attrgetter(
                tableModel.c, x) if x else x for x in _select]
        else:
            _fields = [tableModel]

        result = select(_fields).where(
            and_(*_where)).group_by(*_groupby).order_by(*_orderby)
        Logging.debuglog(result)
        return self.dbo.query(result)

    def postMethod(self, SCHEMA_: str, TABLE_: str, data):
        tableModel = self.getModel(SCHEMA_, TABLE_)
        result = tableModel.insert().values(**data)
        return self.dbo.execute(result)

    def delMethod(self, SCHEMA_: str, TABLE_: str, params):
        def attrgetter(obj, att): return operator.attrgetter(att)(obj)
        tableModel = self.getModel(SCHEMA_, TABLE_)

        _where = [attrgetter(tableModel.c, key) == params.get(key)
                  for key in params if key not in self.keywordSet]
        result = tableModel.delete().where(and_(*_where))
        return self.dbo.execute(result)

    def putMethod(self, SCHEMA_: str, TABLE_: str, params, data):
        def attrgetter(obj, att): return operator.attrgetter(att)(obj)
        tableModel = self.getModel(SCHEMA_, TABLE_)

        _where = [attrgetter(tableModel.c, key) == params.get(key)
                  for key in params if key not in self.keywordSet]
        result = tableModel.update().where(and_(*_where)).values(**data)
        return self.dbo.execute(result)

    def getModel(self, SCHEMA_: str, TABLE_: str) -> Table:
        if not DatabaseOperate.engine:
            return None
        metadata = MetaData(DatabaseOperate.engine)
        _table = Table(TABLE_, metadata, schema=SCHEMA_, autoload=True)
        return _table
