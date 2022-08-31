"""
@author: yichao.deng
@file: db.py
@time: 2022.08.31  
"""  # noqa

import pymysql
from loguru import logger


class MySql(object):
    """
    Mysql 工具类
    """
    LOCK_READ = 0
    LOCK_WRITE = 1

    def __init__(
            self,
            conf: dict = None,
            debug=False,
            charset='utf8mb4',
            batch_size=100
    ):
        """
        初始化 Mysql 工具类
        :param conf:
        :param debug:
        """
        if not isinstance(conf, dict):
            raise TypeError("Error: config require dict type")
        self.conf = conf
        self.debug = debug
        self.charset = charset
        self.auto_commit = True
        self.batch_size = batch_size

    def _init_conf(self):
        """
        初始化 db 实例
        :return: None
        """
        conf = self.conf.copy()
        self.db, self.cursor = self._connect(conf)

    def _connect(self, spec: dict):
        """
        连接数据库
        :param spec:
        :return:
        """
        try:
            db = pymysql.connect(**spec)
            cursor = db.cursor()
            cursor.execute(f"SET NAMES {self.charset}")
        except Exception:
            raise Exception("Error: import mysql error")
        return db, cursor

    def _reconnect(self):
        self._connect(self.conf)

    def reconnect(self):
        self._connect(self.conf)

    @property
    def status(self):
        """
        获取连接状态
        :return:
        """
        return self.db.open

    def close(self):
        """
        关闭数据库连接
        :return:
        """
        self.db.close()

    def insert(self, entry, table, ignore=False, replace=False):

        entry_list = [entry] if isinstance(entry, dict) else entry
        if not isinstance(entry_list, list) or not entry_list:
            raise TypeError("Error: entry type should be dict or not None")
        fields = ", ".join(entry_list[0])

        def func(entry_list):  # noqa
            statement = str.format("({0})",
                                   ", ".join(str.format("\"{0}\"",
                                                        self.sql_escape(entry_list[k]))
                                             if entry_list[k] is not None else "NULL"
                                             for k in entry_list))
            return statement

        def func2(table, fields, entry_list):  # noqa
            statement = str.format("{3} {4} INTO {0} ({1}) VALUES {2}", table, fields, ", ".join(map(func, entry_list)),
                                   # noqa
                                   'INSERT' if not replace else "REPLACE",
                                   ' IGNORE ' if ignore else '')
            res = self.cursor.execute(statement)
            if self.debug:
                logger.info(self.conf['user'], statement, res)

            if self.auto_commit:
                self.commit()
            return res

        data_len = len(entry_list)
        insert_offset = 0
        column_num = 0
        while data_len // self.batch_size:
            tmp_list = entry_list[insert_offset: (insert_offset + self.batch_size)]
            num = func2(table, fields, tmp_list)
            column_num += num
            data_len -= self.batch_size
            insert_offset += self.batch_size

        if data_len:
            final_list = entry_list[insert_offset:]
            tmp_num = func2(table, fields, final_list)
            column_num += tmp_num
        return column_num

    @staticmethod
    def sql_escape(statement):
        if not isinstance(statement, str):
            return statement
        return pymysql.escape_string(statement)

    def commit(self):
        try:
            self.cursor.execute("commit")
            self.auto_commit = True
        except:  # noqa
            self._reconnect()
            self.cursor.execute("commit")
            self.auto_commit = True
