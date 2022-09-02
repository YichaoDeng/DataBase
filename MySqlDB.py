"""
@author: yichao.deng
@file: MysqlDB.py
@time: 2022-09-02
"""  # noqa

import subprocess
import time
from pprint import pprint

import pymysql
import traceback
from loguru import logger

USER = "root"
PASSWORD = "Zz123456"
HOST = "127.0.0.1"
DATABASE = "test"
PORT = 3306

mysql_config = {
    'host': HOST,
    'user': USER,
    'password': PASSWORD,
    'database': DATABASE,
    'port': PORT,
    'charset': 'utf8mb4',
    'autocommit': True
}

logger.add(sink='log.txt', format="{time}, {level}, {message}", level="INFO")


def mysql_log_wrapper(func):
    """
    mysql 日志装饰器
    :param func:
    :return:
    """

    def inner(*args, **kwargs):
        timestamp = int(time.time() * 1000)
        ret = func(*args, **kwargs)  # noqa
        user = args[0].basic_config.get('user')
        sql = args[1]
        logger.info(f"{timestamp}, {user}, {sql}, {ret}")
        return ret

    return inner


class Singleton(type):
    """
    单例模式
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class MySQL(metaclass=Singleton):
    """
    MySQL 工具类
    """

    def __init__(self,
                 basic_config: dict = None,
                 **kwargs
                 ) -> None:
        """
        初始化 mysql 连接配置
        :param basic_config:
        :param kwargs:
        """
        self.basic_config = basic_config
        self.kwargs = kwargs
        self.cursor_type = kwargs.pop('cursor_type') if kwargs.get('cursor_type') else 'set'
        self.__connect()
        self.__create_cursor()

    def __connect(self):
        """
        连接 mysql
        :return:
        """
        self.__conn = pymysql.connect(
            **self.basic_config
        )

    def __create_cursor(self):
        """
        创建游标
        :return:
        """
        self.cursor = self.__conn.cursor() if \
            self.cursor_type == 'set' else \
            self.__conn.cursor(
                pymysql.cursors.DictCursor)  # noqa

    @property
    def status(self):
        """
        获取 db 连接状态
        :return:
        """
        return self.__conn.open

    @mysql_log_wrapper
    def execute_sql(self, sql):
        """
        执行 sql 语句
        :param sql:
        :return:
        """
        try:
            self.cursor.execute(sql)
        except Exception:  # noqa
            print(f"sql语句执行出错,sql是:{sql},报错信息:{traceback.format_exc()}")
            return False
        else:
            return True

    def fetchone(self, sql):
        """
        获取一条数据
        :param sql:
        :return:
        """
        sql_tag = self.execute_sql(sql)
        if sql_tag:
            result = self.cursor.fetchone()
            return result

    def fetchall(self, sql):
        """
        获取所有数据
        :param sql:
        :return:
        """
        sql_tag = self.execute_sql(sql)
        if sql_tag:
            result = self.cursor.fetchall()
            return result

    def __close(self):
        """
        关闭游标连接
        :return:
        """
        if hasattr(self, "conn"):
            self.__conn.close()
            self.cursor.close()


class ServerTools:
    """
    服务器工具类
    """

    @classmethod
    def run_command(cls, command, timeout=10):
        """
        服务器 指令执行器
        :param command: 执行
        :param timeout: 超时时间
        :return:
        """
        popen = subprocess.Popen(
            command,
            encoding="utf-8",
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True
        )
        try:
            result, error = popen.communicate(timeout=timeout)
        except:  # noqa
            result = ''
            error = '获取返回值超时, 请检查是否为阻塞型任务'

        return {
            'code': popen.returncode,
            'msg': error or "success",
            'result': result,
        }

    @classmethod
    def run_port_monitor(cls, port, reset=False):
        """
        运行 端口监控统计
        :param port: 端口号
        :param reset: 是否重置输入输出统计
        :return:
        """
        if reset:
            cls.run_command(f'iptables -Z INPUT')
            cls.run_command(f'iptables -Z OUTPUT')

        cls.run_command(f'iptables -A INPUT -p tcp --dport {port}')  # noqa
        cls.run_command(f'iptables -A OUTPUT -p tcp --sport {port}')

    @classmethod
    def get_port_info(cls, port, reset=False):
        """
        获取 端口流量统计信息
        :param port: 端口号
        :param reset: 是否重置输入输出统计
        :return:
        """
        cls.run_port_monitor(port=port, reset=reset)
        return cls.run_command(f'iptables -L -v -n -x')


if __name__ == '__main__':
    mysql = MySQL(basic_config=mysql_config, cursor_type='dict')
    ret_0 = mysql.fetchone(sql='xxxxxxx')  # noqa
    ret_1 = mysql.fetchone(sql='select * from test_table limit 10')  # noqa
    ret_2 = mysql.fetchall(sql='select * from test_table limit 10')  # noqa
    print(f"""
    ret_0:
        {ret_0}
    ret_1:
        {ret_1}
    ret_2:
        {ret_2}
    """)

    # server = ServerTools()
    # ret = server.get_port_info(port=3306)
    # pprint(ret)
