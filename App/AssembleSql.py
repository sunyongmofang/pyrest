from sqlalchemy import text
import logging


class AssembleSql(object):

    def __init__(self):
        self.SELECT = 'select'
        self.FROM = 'from'
        self.WHERE = 'where'
        self.JOIN = 'join'
        self.ON = 'on'
        self.GROUPBY = 'group by'
        self.ORDERBY = 'order by'
        self.LEFT = 'left'
        self.RIGHT = 'right'
        self.LIMIT = 'limit'
        self.OFFSET = 'offset'
        self.AND = 'and'
        self.COUNT = 'count'

    def getMethod(self, DATABASE_, SCHEMA_, TABLE_, params):
        keyword = ['_select', '_count', '_page', '_page_size', '_groupby']
        values = [params.get(x) for x in keyword]
        asterisk = '*'
        empty = ''
        result = None
        whereMap = {}

        tablename = '{DATABASE_}.{SCHEMA_}.{TABLE_}'.format(DATABASE_=DATABASE_, SCHEMA_=SCHEMA_, TABLE_=TABLE_)
        _select = values[0] if values[0] else asterisk
        _count = '{COUNT}({_count})'.format(COUNT=self.COUNT, _count=values[1]) if values[1] else empty
        _page = None
        _page_size = None
        _groupby = values[4]
        _fields = _count if _count else _select
        if values[2] and values[3]:
            _page = values[2]
            _page_size = values[3]

        result = '{SELECT} {_fields} {FROM} {tablename}'.format(
            SELECT=self.SELECT,
            _fields=_fields,
            FROM=self.FROM,
            tablename=tablename
        )

        _where = []
        for key in params:
            if key not in keyword:
                value = params.get(key)
                _where.append('{key} = :{key}'.format(key=key))
                whereMap[key] = value
        if _where:
            result += ' {WHERE} '.format(WHERE=self.WHERE) + \
                ' {AND} '.format(AND=self.AND).join(_where)
        if _groupby:
            result += ' {GROUPBY} {_groupby}'.format(GROUPBY=self.GROUPBY, _groupby=_groupby)
        logging.basicConfig(level=logging.DEBUG)
        logging.debug(result)
        return text(result), whereMap
