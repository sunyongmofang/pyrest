from App import AssembleSql
from App import DatabaseOperate


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


