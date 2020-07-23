from App import AssembleSql
from App import DatabaseOperate
from App import Config
from App import Logging
from sqlalchemy import text


if __name__ == '__main__':

    Config('./prest.toml')
    config = Config.config

    Logging(__name__)
    debuglog = Logging.debuglog
    infolog = Logging.infolog

    # debuglog(config.http.port)
    # debuglog(config.jwt.default)
    # debuglog(config.jwt.key)
    # debuglog(config.jwt.algo)
    # debuglog(config.database.host)
    # debuglog(config.database.port)
    # debuglog(config.database.user)
    # debuglog(config.database._pass)
    # debuglog(config.database.database)
    # debuglog(config.database.URL)
    # debuglog(config.queries.location)
    # debuglog(config.debug)

    dbo = DatabaseOperate()
    asm = AssembleSql()

    # params = {
        # '_count': 'f_student_name',
        # '_select': 'f_student_id',
        # '_groupby': 'f_student_name',
        # 'f_student_name': '张三',
        # 'f_student_id': '1'
    # }
    data = {
        'f_student_name': 'laola'
    }

    updateParams = {
        'f_student_id': 11
    }
    updateData = {
        'f_student_name': 'laola11'
    }

    # res = asm.getMethod('public', 't_student', {})
    # infolog('1.显示全部数据：\n')
    # infolog(res)

    # res = asm.getMethod('public', 't_student', {
    #     '_select': 'f_student_id'
    # })
    # infolog('2.只展现学生id：\n')
    # infolog(res)

    # res = asm.getMethod('public', 't_student', {
    #     '_select': '*'
    # })
    # infolog('3.显示全部数据：\n')
    # infolog(res)

    # res = asm.getMethod('public', 't_student', {
    #     '_count': '*'
    # })
    # infolog('4.count*：\n')
    # infolog(res)

    # res = asm.getMethod('public', 't_student', {
    #     '_count': 'f_student_id'
    # })
    # infolog('5.count f_student_id：\n')
    # infolog(res)

    # res = asm.getMethod('public', 't_student', {
    #     '_page': '4',
    #     '_page_size': '10',
    #     '_orderby': '-f_student_id'
    # })
    # infolog('6.分页第一页：\n')
    # infolog(res)

    # res = asm.getMethod('public', 't_student', {
    #     'f_student_id': '2'
    # })
    # infolog('7.查询f_student_id为2的结果：\n')
    # infolog(res)

    # insertObject = asm.postMethod('public', 't_student', data)
    # res = dbo.execute(insertObject)

    # sqlObj = asm.putMethod('public', 't_student', updateParams, updateData)
    # res = dbo.execute(sqlObj)

    




