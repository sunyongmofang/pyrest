import operator

from sqlalchemy import text
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import Table

from .DatabaseOperate import DatabaseOperate


class TableMeta(object):

    def __init__(self, SCHEMA_: str, TABLE_: str):
        self.SCHEMA_ = SCHEMA_
        self.TABLE_ = TABLE_
        self.keyword = [
            '_select',
            '_count',
            '_page',
            '_page_size',
            '_groupby',
            '_order'
        ]
        self.keywordSet = set(self.keyword)

        self.getModel()
        self.getField()

    def getModel(self):
        if not DatabaseOperate.engine:
            return None
        metadata = MetaData(DatabaseOperate.engine)
        self._table = Table(self.TABLE_, metadata, schema=self.SCHEMA_, autoload=True)

    def getField(self):
        self.field = {}
        for x in self._table.c:
            self.field[x.name] = x

    def getSelect(self, params: dict):
        res = [self._table]

        _select = params.get('_select')
        _count = params.get('_count')

        if _count:
            if _count == '*':
                res = [func.count([self.field[x] for x in self.field][0]).label('count')]
            elif _count in self.field:
                res = [func.count(self.field[_count]).label('count')]
        elif _select and _select != '*':
            res = []
            funcMap = {
                'sum': func.sum,
                'count': func.count,
                'avg': func.avg,
                'max': func.max,
                'min': func.min,
                'median': func.median,
                'stddev': func.stddev,
                'variance': func.variance
            }
            for x in _select.split(','):
                if x in self.field:
                    res.append(self.field[x])
                elif len(x.split(':')) == 2 and x.split(':')[1] in self.field:
                    aggFunc, fieldStr = x.split(':')
                    res.append(funcMap[aggFunc](self.field[fieldStr]).label(aggFunc))

        return res

    def getGroupby(self, params: dict):
        res = [None]

        _groupby = params.get('_groupby')

        if _groupby:
            res = [self.field[x] for x in _groupby.split(',') if x in self.field]

        return res

    def getOrderby(self, params: dict):
        res = [None]

        _order = params.get('_order')

        if _order:
            res = [self.field[x[1:]].asc() if '-' == x[0] else self.field[x].desc() for x in _order.split(',') if x in self.field or x[1:] in self.field]

        return res

    def getPage(self, params: dict):
        _limit = None
        _offset = None

        _page = params.get('_page')
        _page_size = params.get('_page_size')

        if _page and _page_size and str(_page).isdigit() and str(_page_size).isdigit():
            _limit = int(_page_size)
            _offset = _limit * (int(_page) - 1)

        return _limit, _offset

    def getWhere(self, params: dict):
        res = []
        for x in params:
            if x in self.keywordSet or x not in self.field:
                continue
            cValue = params[x]
            fieldObj = self.field[x]
            fx = self.whereList(fieldObj, cValue)
            res.append(fx)
        return res

    def whereList(self, field, cValue: str) -> text:
        keyList = ['$eq', '$gt', '$gte', '$lt', '$lte', '$ne', '$in', '$nin', '$null', '$notnull', '$true', '$nottrue', '$false', '$notfalse', '$like', 'i$like', '$notlike', '$notilike']
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
                result = field.is_(None)
            if '$notnull' == condition[0]:
                result = field.isnot(None)
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
            if '$notlike' == condition[0]:
                result = field.notlike(condition[1])
            if '$notilike' == condition[0]:
                result = field.notilike(condition[1])
        else:
            result = field == cValue

        return result
