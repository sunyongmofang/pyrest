from App import AssembleSql
from App import DatabaseOperate
from App import Config


if __name__ == '__main__':
    asm = AssembleSql()
    params = {
        # '_count': 'f_student_name',
        # '_select': 'f_student_id',
        # '_groupby': 'f_student_id',
        'f_student_id': '1',
        'f_student_name': '张三'
    }
    sqlStr, whereMap = asm.getMethod('test', 'public', 't_student', params)

    dbo = DatabaseOperate()
    res = dbo.execute(sqlStr, whereMap)
    print(res)

    config = Config.config
    if not Config.config:
        Config('./prest.toml')
        config = Config.config

    print(config.http.port)
    print(config.jwt.default)
    print(config.jwt.key)
    print(config.jwt.algo)
    print(config.database.host)
    print(config.database.port)
    print(config.database.user)
    print(config.database._pass)
    print(config.database.database)
    print(config.database.URL)
    print(config.queries.location)


