import json
import logging
import traceback
import re
import os.path

from ipykernel.kernelbase import Kernel
from sqlalchemy.exc import OperationalError, ResourceClosedError

from .constants import __version__, KERNEL_NAME, CONFIG_FILE

from sqlalchemy import *
import pandas as pd
from .tool_sql import *

import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class KernelSyntaxError(Exception):
    pass


error_con_not_created = """Connection not initialized!
Please specify your pyHive configuration like this :

-------------
$$ url=hive://<kerberos-username>@<hive-host>:<hive-port>/<db-name>
$$ connect_args={"auth": "KERBEROS","kerberos_service_name": "hive"}
$$ pool_size=5
$$ max_overflow=10

YOUR SQL REQUEST HERE IF ANY
-------------

-> if you want to update the current connection, just type it again with another configuration
-> $$ are mandatory characters that specify that this line is a configuration for this kernel

Other parameters are available such as :

$$ default_limit=50 # -> without this parameter, default_limit is set to 20
$$ display_mode=be # -> this will display a table with the beginning (b) and end (e) of the SQL response (options are: b, e and be)

"""


class ConnectionNotCreated(Exception):
    def __init__(self):
        Exception.__init__(self, error_con_not_created)


class HiveQLKernel(Kernel):
    implementation = KERNEL_NAME
    implementation_version = __version__
    banner = 'HiveQL REPL'
    language = "hiveql"
    language_info = {
        'name': 'hive',
        'codemirror_mode': "sql",
        'pygments_lexer': 'postgresql',
        'mimetype': 'text/x-hive',
        'file_extension': '.hiveql',
    }
    last_conn = None
    params = {
        "default_limit": 20,
        "display_mode": "be"
    }
    conf = None
    conf_file = os.path.expanduser(CONFIG_FILE)
    if os.path.isfile(conf_file):
        with open(conf_file, mode='r') as file_hanlde:
            conf = json.load(file_hanlde)

    def send_exception(self, e):
        if type(e) in [ConnectionNotCreated]:
            tb = ""
        else:
            tb = "\n" + traceback.format_exc()
        return self.send_error(str(e) + tb)

    def send_error(self, contents):
        self.send_response(self.iopub_socket, 'stream', {
            'name': 'stderr',
            'text': str(contents)
        })
        return {
            'status': 'error',
            'execution_count': self.execution_count,
            'payload': [],
            'user_expressions': {}
        }

    def send_info(self, contents):
        self.send_response(self.iopub_socket, 'stream', {
            'name': 'stdout',
            'text': str(contents)
        })

    def create_conn(self, url, **kwargs):
        self.send_info("create_engine('" + url + "', " + ', '.join(
            [str(k) + '=' + (str(v) if type(v) == str else json.dumps(v)) for k, v in kwargs.items()]) + ")\n")
        self.last_conn = create_engine(url,**kwargs)
        self.last_conn.connect()
        self.send_info("Connection established to database!\n")

    def reconfigure(self, params):
        if 'default_limit' in params:
            try:
                self.params['default_limit'] = int(params['default_limit'])
                self.send_info("Set display limit to {}\n".format(self.params['default_limit']))
            except ValueError as e:
                self.send_exception(e)
        if 'display_mode' in params:
            v = params['display_mode']
            if type(v) == str and v in ['b', 'e', 'be']:
                self.params['display_mode'] = v
            else:
                self.send_error("Invalid display_mode, options are b, e and be.")

    def parse_code(self, code):
        req = code.strip()

        headers = {}
        sql_req = ""
        beginning = True
        for l in req.split('\n'):
            l = l.strip()
            if l.startswith("$$"):
                if beginning:
                    k, v = l.replace("$", "").split("=")
                    k, v = k.strip(), v.strip()
                    if v.startswith('{'):
                        v = json.loads(v)
                    else:
                        try:
                            v = int(v)
                        except ValueError:
                            pass
                    headers[k] = v
                else:
                    raise KernelSyntaxError("Headers starting with %% must be at the beginning of your request.")
            else:
                beginning = False
                sql_req += '\n' + l

        if self.last_conn is None and not headers and self.conf is not None:
            headers = self.conf  # if cells doesn't contain $$ and connection is None, overriding headers with conf data

        sql_req = sql_req.strip()
        if sql_req.endswith(';'):
            sql_req = sql_req[:-1] + "\n" # the last newline let add the limit without being commented by a last comment

        a = ['default_limit', 'display_mode']
        params, pyhiveconf = {k: v for k, v in headers.items() if k in a}, {k: v for k, v in headers.items() if k not in a}

        self.reconfigure(params)

        return pyhiveconf, sql_req

    def format_time(self, start, end):
        hours, rem = divmod(end-start, 3600)
        minutes, seconds = divmod(rem, 60)
        return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        try:
            pyhiveconf, sql_req = self.parse_code(code)

            if 'url' in pyhiveconf:
                self.create_conn(**pyhiveconf)

            if self.last_conn is None:
                raise ConnectionNotCreated()

            # If code empty
            if not sql_req:
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            pd.set_option('display.max_colwidth', -1)
            sql_req = sql_remove_comment(sql_req)

            for query_raw in sql_explode(sql_req):
                query = sql_rewrite(query_raw, self.params['default_limit'])
                logger.info("Running the following HiveQL query: {}".format(query))
                start = time.time()
                result = self.last_conn.execute(query.strip())
                end = time.time()
                elapsed_time = self.format_time(start, end)
                if result is not None and result.returns_rows is True:
                    df = pd.DataFrame(result.fetchall(), columns=result.keys(), dtype="object")
                    if sql_is_show(query) or sql_is_describe(query): # allow limiting show tables/databases and describe table with a pattern
                        pattern = extract_pattern(query_raw)
                        if sql_is_describe(query):
                            df = df[df.col_name.str.contains(pattern)]
                        if sql_is_show_tables(query):
                            df = df[df.tab_name.str.contains(pattern)]
                        if sql_is_show_databases(query):
                            df = df[df.database_name.str.contains(pattern)]
                    html = df_to_html(df)
                    self.send_info("Elapsed Time: {} !\n".format(elapsed_time))
                    self.send_response(self.iopub_socket, 'display_data', {
                        'data': {
                            "text/html": html,
                        },
                        "metadata": {
                            "image/png": {
                                "width": 640,
                                "height": 480,
                            },
                        }
                    })
                else:
                    if sql_is_use(query):
                        self.send_info("Database changed successfully in {} !\n".format(elapsed_time))
                    elif sql_is_create(query):
                        self.send_info("Table created successfully in {} !\n".format(elapsed_time))
                    elif sql_is_drop(query):
                        self.send_info("Table dropped successfully in {} !\n".format(elapsed_time))
                    elif sql_is_set_variable(query):
                        self.send_info("Variable set successfully in {} !\n".format(elapsed_time))
                    else:
                        self.send_info("Query executed successfully in {} !\n".format(elapsed_time))
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        except OperationalError as oe:
            return self.send_error(refactor(oe))
        except ResourceClosedError as rce:
            return self.send_error(rce)
        except NotAllowedQueriesError as e:
            return self.send_error("only 'select', 'with', 'set property=value', 'create table x.y stored as orc' 'drop table', 'use database', 'show databases', 'show tables', 'describe myTable' statements are allowed")
        except Exception as e:
            return self.send_exception(e)


def df_to_html(df):
    #for column in df:
    #    if df[column].dtype == 'object':
    #        df[column] =  df[column].apply(lambda x: x.replace("\n","<br>"))
    return df.fillna('NULL').astype(str).to_html(notebook=True)


def refactor(oe):
    error_string = "error_code: {}\nsql_state: {}\nerror_message: {}".format(oe.orig.args[0].status.errorCode,
                                                                             oe.orig.args[0].status.sqlState,
                                                                             oe.orig.args[0].status.errorMessage)
    return error_string
