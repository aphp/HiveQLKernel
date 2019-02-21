from hiveql.tool_sql import *
import logging 

def test_validate():
    SQL = "select * from toto; "
    assert sql_validate(SQL) == None
    BAD_SQL = "select * from toto; select * from tata;"
    try:
        sql_validate(BAD_SQL)
        assert False
    except MultipleQueriesError:
        assert True
    SQL = "drop table toto; "
    assert sql_validate(SQL) == None
    SQL = "with t as (select * from tata) select * from t; "
    assert sql_validate(SQL) == None
    SQL = "show databases"
    assert sql_validate(SQL) == None
    SQL = "describe extended toto"
    assert sql_validate(SQL) == None
    SQL = "describe a.toto"
    assert sql_validate(SQL) == None
    SQL = "show databases mydatabase"
    assert sql_validate(SQL) == None
    SQL = "SET hive.enforce.sorting = true"
    assert sql_validate(SQL) == None

def test_extract_limit():
    SQL = "select * from t limit 200;"
    assert sql_extract_limit(SQL) == 200
    SQL = "select * from (select * from b limit 20000) t limit 1000"
    assert sql_extract_limit(SQL) == 1000
    SQL = "select * from (select * from b limit 20000) t "
    assert sql_extract_limit(SQL) == 0

def test_incrust_limit():
    DEFAULT_LIMIT = 100
    SQL = "select * from t limit 200;"
    assert sql_incrust_limit(SQL, DEFAULT_LIMIT) == "select * from t  limit 100"
    SQL = "select * from t ;"
    assert sql_incrust_limit(SQL, DEFAULT_LIMIT) == "select * from t  limit 100"
    SQL = "select * from t "
    assert sql_incrust_limit(SQL, DEFAULT_LIMIT) == SQL + " limit 100"
    SQL = "select * from (select * from t limit 100 as t) "
    assert sql_incrust_limit(SQL, DEFAULT_LIMIT) == SQL + " limit 100"
    SQL = "with t as (select * from t limit 100) select * from t"
    assert sql_incrust_limit(SQL, DEFAULT_LIMIT) == SQL + " limit 100"

def test_is_selection():
    SQL = "select * from t limit 200;"
    assert sql_is_selection(SQL)
    SQL = " witH a as (select * from t) select * from a limit 200;"
    assert sql_is_selection(SQL)
    SQL = "create * from t limit 200;"
    assert sql_is_selection(SQL) == None
    SQL = "--hello world\n--second\nselect * from toto; "
    assert sql_is_selection(SQL)
    SQL = "--hello world\n--second\nselect * from toto; "
    assert sql_validate(SQL) == None

def test_is_create():
    SQL = "create table toto stored as orc as select * from t;"
    assert sql_is_create(SQL)
    SQL = "    create table toto  as select * from t;"
    assert sql_is_create(SQL) == None

def test_is_create():
    SQL = " drop table toto stored as orc as select * from t;"
    assert sql_is_drop(SQL)
    SQL = " drip table toto stored as orc as select * from t;"
    assert sql_is_drop(SQL) == None

def test_is_show():
    SQL = " show databases;"
    assert sql_is_show(SQL)
    SQL = "show databases toto"
    assert sql_is_show(SQL)

def test_is_describe():
    SQL = "describe  a.toto"
    assert sql_is_describe(SQL)
    SQL = "show my databases"
    assert sql_is_describe(SQL) == None

def test_rewrite():
    DEFAULT_LIMIT = 100
    SQL = "select * from t limit 200;"
    assert sql_rewrite(SQL, DEFAULT_LIMIT) == "select * from t  limit 100"
    SQL = "show databases cse"
    assert sql_rewrite(SQL, DEFAULT_LIMIT) == "show databases"
    SQL = "show tables cse"
    assert sql_rewrite(SQL, DEFAULT_LIMIT) == "show tables"
    SQL = "select count(1)  from t"
    assert sql_rewrite(SQL, DEFAULT_LIMIT) == "select count(1)  from t"
    SQL = "select count(*)  from t"
    assert sql_rewrite(SQL, DEFAULT_LIMIT) == "select count(*)  from t"

def test_rewrite2():
    SQL = "show tables cse"
    assert extract_show_pattern(SQL) == "cse"

def test_explain():
    SQL = "explain select * from toto"
    assert sql_is_explain(SQL) 

def test_remove_comment():
    SQL = "--hello world\n--second\nselect * from toto; "
    assert sql_remove_comment(SQL) == "select * from toto; "

