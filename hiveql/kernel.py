import json
import logging
import traceback

from ipykernel.kernelbase import Kernel
from sqlalchemy.exc import OperationalError

from .constants import __version__, KERNEL_NAME

from sqlalchemy import *
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class KernelSyntaxError(Exception):
    pass


error_con_not_created = """Connection not initialized!
Please specify your pyHive configuration like this (if you want to update the current connection, just type it again with another configuration):

-------------
$$ url=hive://<kerberos-username>@<hive-host>:<hive-port>/<db-name>
$$ connect_args={"auth": "KERBEROS","kerberos_service_name": "hive"}
$$ pool_size=5
$$ max_overflow=10

YOUR SQL REQUEST HERE IF ANY
-------------
"""


class ConnectionNotCreated(Exception):
    def __init__(self):
        Exception.__init__(self, error_con_not_created)


class HiveQLKernel(Kernel):
    implementation = KERNEL_NAME
    implementation_version = __version__
    banner = 'HiveQL REPL'
    language_info = {
        'name': 'hive',
        'codemirror_mode': 'python',
        'pygments_lexer': 'sql',
        'mimetype': 'text/plain',
        'file_extension': '.hiveql',
    }
    last_conn = None

    def send_exception(self, e):
        tb = traceback.format_exc()
        return self.send_error(str(e) + '\n' + tb)

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
        self.last_conn = create_engine(url, **kwargs)
        self.last_conn.connect()
        self.send_info("Connection established to database!\n")

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
                sql_req += ' ' + l

        sql_req = sql_req.strip()
        if sql_req.endswith(';'):
            sql_req = sql_req[:-1]

        return headers, sql_req

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        try:
            headers, sql_req = self.parse_code(code)

            if 'url' in headers:
                self.create_conn(**headers)

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

            html = pd.read_sql(sql_req, self.last_conn).to_html()
        except OperationalError as oe:
            return self.send_error(oe)
        except Exception as e:
            return self.send_exception(e)

        # msg_types = https://jupyter-client.readthedocs.io/en/latest/messaging.html?highlight=stream#messages-on-the-iopub-pub-sub-channel
        self.send_response(self.iopub_socket, 'execute_result', {
            "execution_count": self.execution_count,
            'data': {
                "text/html": html,
            },
        })

        return {
            'status': 'ok',
            'execution_count': self.execution_count,
            'payload': [],
            'user_expressions': {}
        }
