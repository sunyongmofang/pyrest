import logging
import operator

from sqlalchemy import text
from sqlalchemy import func
from sqlalchemy import desc
from sqlalchemy import asc
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.sql import update
from sqlalchemy.sql import delete
from sqlalchemy.sql import select
from sqlalchemy.sql import insert
from sqlalchemy.sql import and_

from .Config import Config
from .Logging import Logging
from .DatabaseOperate import DatabaseOperate


class AssembleSql(object):

    def __init__(self):
        self.STAR = '*'
        self.keyword = ['_select', '_count', '_page',
                        '_page_size', '_groupby', '_orderby']
        self.keywordSet = set(self.keyword)
        self.dbo = DatabaseOperate()

    def getMethod(self, SCHEMA_: str, TABLE_: str, params: dict):
        def attrgetter(obj, att): return operator.attrgetter(att)(obj)
        tableModel = self.getModel(SCHEMA_, TABLE_)
        values = [params.get(x) for x in self.keyword]
        result = None

        _select = values[0].split(',') if values[0] else None
        _count = None
        if values[1] and values[1] is not self.STAR:
            _count = [func.count(
                attrgetter(tableModel.c, values[1])
            ).label('count')]
        elif values[1] is self.STAR:
            _count = [func.count([i for i in tableModel.c][0]).label('count')]

        _page = None
        _page_size = None
        _groupby = values[4].split(',') if values[4] else [None]

        _orderby = [None]
        if values[5]:
            _orderby = [asc(attrgetter(tableModel.c, x[1:])) if '-' == x[0] else desc(attrgetter(tableModel.c, x)) for x in values[5].split(',')]

        if values[2] and values[3] and values[2].isdigit() and values[3].isdigit() and int(values[2]) >= 1 and int(values[3]) >= 1:
            _page = int(values[2])
            _page_size = int(values[3])

        _where = [self.whereList(attrgetter(tableModel.c, key), params.get(key))
                  for key in params if key not in self.keywordSet]

        _fields = None
        if _select or _count:
            _fields = _count if _count else [attrgetter(
                tableModel.c, x) if x and x is not self.STAR else tableModel for x in _select]
        else:
            _fields = [tableModel]

        result = select(_fields) \
            .where(and_(*_where)) \
            .group_by(*_groupby) \
            .order_by(*_orderby) \
            .limit(_page_size if _page_size else None) \
            .offset(_page_size * (_page - 1) if _page_size else None)
        Logging.debuglog(result)
        return self.dbo.query(result)

    def postMethod(self, SCHEMA_: str, TABLE_: str, data: dict):
        tableModel = self.getModel(SCHEMA_, TABLE_)
        result = insert(tableModel).values(**data)
        return self.dbo.execute(result)

    def delMethod(self, SCHEMA_: str, TABLE_: str, params: dict):
        def attrgetter(obj, att): return operator.attrgetter(att)(obj)
        tableModel = self.getModel(SCHEMA_, TABLE_)

        _where = [self.whereList(attrgetter(tableModel.c, key), params.get(key))
                  for key in params if key not in self.keywordSet]
        result = delete(tableModel).where(and_(*_where))
        return self.dbo.execute(result)

    def putMethod(self, SCHEMA_: str, TABLE_: str, params: dict, data: dict):
        def attrgetter(obj, att): return operator.attrgetter(att)(obj)
        tableModel = self.getModel(SCHEMA_, TABLE_)

        _where = [self.whereList(attrgetter(tableModel.c, key), params.get(key))
                  for key in params if key not in self.keywordSet]
        result = update(tableModel).where(and_(*_where)).values(**data)
        return self.dbo.execute(result)

    def getModel(self, SCHEMA_: str, TABLE_: str) -> Table:
        if not DatabaseOperate.engine:
            return None
        metadata = MetaData(DatabaseOperate.engine)
        _table = Table(TABLE_, metadata, schema=SCHEMA_, autoload=True)
        return _table

    def whereList(self, field, cValue: str) -> text:
        keyList = ['$eq', '$gt', '$gte', '$lt', '$lte', '$ne', '$in', '$nin',
                   '$null', '$notnull', '$true', '$nottrue', '$false', '$notfalse', '$like', 'i$like']
        condition = cValue.split('.')
        if condition[0] in set(keyList):
            if '$eq' == condition[0]:
                result = operator.eq(field, condition[1])
            if '$gt' == condition[0]:
                result = operator.gt(field, condition[1])
            if '$gte' == condition[0]:
                result = operator.ge(field, condition[1])
            if '$lt' == condition[0]:
                result = operator.lt(field, condition[1])
            if '$lte' == condition[0]:
                result = operator.le(field, condition[1])
            if '$ne' == condition[0]:
                result = operator.ne(field, condition[1])
            if '$in' == condition[0]:
                result = field.in_(condition[1].split(','))
            if '$nin' == condition[0]:
                result = field.notin_(condition[1].split(','))
            if '$null' == condition[0]:
                result = field == None
            if '$notnull' == condition[0]:
                result = field != None
            if '$true' == condition[0]:
                result = field == True
            if '$nottrue' == condition[0]:
                result = field == False
            if '$false' == condition[0]:
                result = field == False
            if '$notfalse' == condition[0]:
                result = field == True
            if '$like' == condition[0]:
                result = field.like(condition[1])
            if '$ilike' == condition[0]:
                result = field.ilike(condition[1])
        else:
            result = field == cValue

        return result
