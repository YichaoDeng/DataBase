import pymysql
import traceback
from loguru import logger


class MySQL(object):

    def __init__(self,
                 host: str = '127.0.0.1',
                 port: int = 3306,
                 user: str = 'root',
                 password: str = None,
                 database: str = 'mysql',
                 charset: str = 'utf8mb4',
                 autocommit: bool = True,
                 *args, **kwargs
                 ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self.autocommit = autocommit
        self.args = args
        self.kwargs = kwargs
        self.__connect()
        self.__create_cursor()

    def __connect(self):
        self.__conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset=self.charset,
            autocommit=self.autocommit,
            *self.args, **self.kwargs
        )

    def __create_cursor(self):
        self.cursor = self.__conn.cursor()

    @property
    def status(self):
        return self.__conn.open

    # @property
    # def cursor(self, **kwargs):
    #     return self.cursor

    # def __connect(self):  # 加下划线就叫私有方法，方法或者变量前面加，就叫私有方法。只能自己调用在这个类里面
    #
    #     try:
    #         self.__conn = pymysql.connect(**self.mysql_info)  #
    #     except Exception:
    #         logger.error("mysql连接失败！mysql的连接信息是{},错误信息,{}",  # 日志
    #                      self.mysql_info, traceback.format_exc())  # 错误信息
    #         quit()
    #
    # def __create_cursor(self):  # 私有
    #     self.cursor = self.__conn.cursor() if self.cur_type == 1 else self.__conn.cursor(pymysql.cursors.DictCursor)

    def execute_sql(self, sql):
        try:
            print(self.cursor.execute(sql))
        except Exception:
            logger.warning("sql语句执行出错,sql是:{},报错信息:{}", sql, traceback.format_exc())
        else:
            return True

    def fetchone(self, sql):
        sql_tag = self.execute_sql(sql)
        if sql_tag:
            result = self.cursor.fetchone()
            logger.debug("本次sql查询的结果是:{}", result)
            return result

    def fetchall(self, sql):
        sql_tag = self.execute_sql(sql)
        if sql_tag:
            result = self.cursor.fetchall()
            logger.debug("本次sql查询的结果是:{}", result)
            return result

    def __close(self):
        if hasattr(self, "conn"):  # 判断当前这个对象里面有没有conn这个属性
            self.__conn.close()
            self.cursor.close()
    # update insert delete  create drop


if __name__ == '__main__':
    # mysql_info = {
    #     "host": "110.40.129.51",
    #     "user": "jxz",
    #     "passwd": "123456",
    #     "db": "jxz",
    #     "port": 3306,
    #     "charset": "utf8",
    #     "autocommit": True
    # }
    # mysql = MySQL(mysql_info)
    # mysql.execute_sql("select * from user_ztt")
    # for line in mysql.cursor:
    #     print(line)
    # mysql.fetchall("select * from user_ztt limit 5;")
    # mysql.fetchone("select * from user_ztt where id =3")
    mysql = MySQL(password='Zz123456', database='test')
    print(mysql.status)
    res = mysql.fetchone(sql='select * from test_table limit 10')
    print(res)
