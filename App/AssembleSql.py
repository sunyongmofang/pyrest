
from sqlalchemy.sql import update
from sqlalchemy.sql import delete
from sqlalchemy.sql import select
from sqlalchemy.sql import insert
from sqlalchemy.sql import and_

from .Logging import Logging
from .DatabaseOperate import DatabaseOperate
from .TableMeta import TableMeta


class AssembleSql(object):

    def __init__(self):
        self.STAR = '*'
        self.keyword = ['_select', '_count', '_page',
                        '_page_size', '_groupby', '_order']
        self.keywordSet = set(self.keyword)
        self.dbo = DatabaseOperate()

    def getMethod(self, SCHEMA_: str, TABLE_: str, params: dict):
        tableMeta = TableMeta(SCHEMA_, TABLE_)

        _select = tableMeta.getSelect(params)
        _where = tableMeta.getWhere(params)
        _groupby = tableMeta.getGroupby(params)
        _orderby = tableMeta.getOrderby(params)
        _limit, _offset = tableMeta.getPage(params)

        result = select(_select) \
            .where(and_(*_where)) \
            .group_by(*_groupby) \
            .order_by(*_orderby) \
            .limit(_limit) \
            .offset(_offset)
        Logging.debuglog(result)
        return self.dbo.query(result)

    def postMethod(self, SCHEMA_: str, TABLE_: str, data: dict):
        tableMeta = TableMeta(SCHEMA_, TABLE_)
        result = insert(tableMeta._table).values(**data)
        Logging.debuglog(result)
        return self.dbo.execute(result)

    def delMethod(self, SCHEMA_: str, TABLE_: str, params: dict):
        tableMeta = TableMeta(SCHEMA_, TABLE_)
        Logging.debuglog(result)
        _where = tableMeta.getWhere(params)

        result = delete(tableMeta._table).where(and_(*_where))
        Logging.debuglog(result)
        return self.dbo.execute(result)

    def putMethod(self, SCHEMA_: str, TABLE_: str, params: dict, data: dict):
        tableMeta = TableMeta(SCHEMA_, TABLE_)
        _where = tableMeta.getWhere(params)

        result = update(tableMeta._table).where(and_(*_where)).values(**data)
        Logging.debuglog(result)
        return self.dbo.execute(result)
