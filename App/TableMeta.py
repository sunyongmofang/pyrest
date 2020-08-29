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
            '_order',
            '_join',
            '_distinct'
        ]
        self.keywordSet = set(self.keyword)
        self.funcMap = {
            'sum': func.sum,
            'count': func.count,
            'avg': func.avg,
            'max': func.max,
            'min': func.min,
            'median': func.median,
            'stddev': func.stddev,
            'variance': func.variance
        }

        self.conditionOperator = {
            '$eq': operator.eq
            '$gt': operator.gt
            '$gte': operator.ge
            '$lt': operator.lt
            '$lte': operator.le
            '$ne': operator.ne
            '$in': lambda (field, x): field.in_(x.split(','))
            '$nin': lambda (field, x): field.notin_(x.split(','))
            '$null': lambda (field, x): field.is_(x)
            '$notnull': lambda (field, x): field.isnot(x)
            '$true': lambda (field, x): field == True
            '$nottrue': lambda (field, x): field == False
            '$false': lambda (field, x): field == False
            '$notfalse': lambda (field, x): field == True
            '$like': lambda (field, x): field.like(x)
            '$ilike': lambda (field, x): field.ilike(x)
            '$notlike': lambda (field, x): field.notlike(x)
            '$notilike': lambda (field, x): field.notilike(x)
        }

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
            for x in _select.split(','):
                fieldObj = self.aggregate(x)
                if str(fieldObj) != 'None':
                    res.append(fieldObj)

        return res

    def getGroupby(self, params: dict):
        gpb = [None]
        hv = []
        groupby = None
        having = None

        _groupby = params.get('_groupby')

        if _groupby:
            breakdown = _groupby.split('->>having:')
            if len(breakdown) == 1:
                groupby = breakdown[0]
            if len(breakdown) == 2:
                groupby, having = breakdown
                for i in having.split(','):
                    aggr = i.split(':')
                    if len(aggr) == 4:
                        fieldStr = '{aggre}:{field}'.format(aggre=aggr[0], field=aggr[1])
                        fieldObj = self.aggregate(fieldStr)
                        if str(fieldObj) == 'None':
                            continue
                        cValue = '{op}.{val}'.format(op=aggr[2], val=aggr[3])
                        condition = self.whereList(fieldObj, cValue)
                        hv.append(condition)
            gpb = [self.field[x] for x in groupby.split(',') if x in self.field]

        hv = hv if len(hv) != 0 else [None]

        return gpb, hv

    def getOrderby(self, params: dict):
        res = [None]

        _order = params.get('_order')

        if _order:
            res = [self.field[x[1:]].asc() if '-' == x[0] else self.field[x].desc() for x in _order.split(',') if x in self.field or x[1:] in self.field]

        return res

    def getDistinct(self, params: dict):
        res = False
        _distinct = params.get('_distinct')
        if _distinct == 'true':
            res = True
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
        condition = cValue.split('.')
        opera, value = [condition[x] if len(condition) > x else None for x in range(2)]
        if opera in self.conditionOperator:
            result = self.conditionOperator[opera](field, value)
        else:
            result = field == cValue

        return result

    def aggregate(self, fieldStr):
        res = None

        if fieldStr in self.field:
            res = self.field[fieldStr]
        elif len(fieldStr.split(':')) == 2 and fieldStr.split(':')[1] in self.field:
            aggFunc, targetField = fieldStr.split(':')
            res = self.funcMap[aggFunc](self.field[targetField]).label(aggFunc)

        return res

