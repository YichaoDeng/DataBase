# coding:utf-8
import pytest
from config import mysql_config
from db import MySqlDB

test = ""

DEBUG = False


def setup_function():
    global test
    mysql_db = MySqlDB(mysql_config, debug=DEBUG)
    print("init database")
    test = mysql_db


def teardown_function():
    global test
    print("close database")
    test.close()


def test_status():
    global test
    print(test.status)


def test_query():
    global test
    sql = """
        select * from test_table limit 10
    """
    data = test.query(sql)
    for item in data:
        print(item['name'])


def test_insert():
    """
        insert one record
    """
    entity = {
        "name": "张三",
        "age": 10,
    }
    entity2 = {
        "name": "王五",
        "age": 20,
    }
    num = test.insert(entity, "test_table")
    num = test.insert(entity2, "test_table")
    print("insert 测试%s" % num)


def test_insert_many():
    " insert more one record"
    data = []
    for i in range(200):
        tmp = {}
        tmp.setdefault("name", "张%s" % i)
        tmp.setdefault("age", i)
        data.append(tmp)
    num = test.insert(data, "test_table")
    print("insert 测试%s" % num)


def test_update():
    "update one record"
    global test
    update_entry = {
        "age": "4",
        "name": "李四"
    }
    cond = {
        "id": "2"
    }
    num = test.update(update_entry, "test_table", cond)
    print("update 测试 %s" % num)


def test_delete():
    "delete record"
    global test
    cond = {
        "id": "1"
    }
    num = test.delete("test_table", cond)
    print("delete 测试%s" % num)


def test_update_many():
    "update one more record"
    global test
    entry = []
    for i in range(7, 200):
        name = "哈哈%s" % i
        tmp = {"name": name, "age": 1000, "update_field": {"id": i}}
        entry.append(tmp)
    # print(entry)
    num = test.update_many(entry, "test_table")
    print("update 测试 %s" % num)


if __name__ == "__main__":
    pytest.main(["-s", "test_db.py"])
