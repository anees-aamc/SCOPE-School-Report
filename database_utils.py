"""
Program Name: database_utils.py
Program Author: Andy Nees
Program Date: 4/1/24
Last Update: 4/10/24
Purpose:

This file contains logic for performing database operations that are preferably not done using
the Pandas functionality of read_sql and to_sql.

"""

import oracledb
import pandas as pd
from sqlalchemy.engine import create_engine


def connect():
    """connect() --> returns a single, non-pooled connection in thick mode."""
    oracledb.init_oracle_client()
    return oracledb.connect(
        user="[SCOPE]",
        password=None,
        dsn="RAEPRODRDS")


def sqlalchemy_engine():
    """Returns a sqlalchemy engine to connect to SCOPE. Used by Pandas."""
    return create_engine("oracle+oracledb://[scope]@raeprodrds")


def get_proxy_user():
    cursor.execute("select sys_context('userenv','proxy_user') user_name from dual")
    result = cursor.fetchone()
    return str(result[0]) if result else None


def drop_table_if_exists(table_name):
    cursor.execute(
        'select table_name from user_tables where table_name = :table_name',
        parameters={'table_name': table_name}
    )
    result = cursor.fetchone()
    if result:
        cursor.execute('drop table :table_name', parameters={'table_name': table_name})


connection = connect()
connection.autocommit = True
cursor = connection.cursor()
cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
engine = sqlalchemy_engine()
username = get_proxy_user()
