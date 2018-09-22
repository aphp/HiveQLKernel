import logging
import string

from ipykernel.kernelbase import Kernel

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

from .constants import __version__, KERNEL_NAME

from random import randint, choice


def gen_fake_array():
    nb_lines = randint(0, 1000)
    nb_cols = randint(0, 150)
    return [''.join(choice(string.ascii_lowercase) for __ in range(randint(2, 15))) for _ in range(nb_cols)], [
        [randint(-1500, + 1500) * randint(0, 1) / randint(1, 9) for __ in range(nb_cols)] for _ in range(nb_lines)
    ]


def array_to_html(titles, array, limit=20):
    result = "<h2>Count = {}</h2><table class='table table-striped'>".format(len(array))
    result += "<thead><tr>"
    for title in titles:
        result += "<th>{}</th>".format(title)
    result += "</tr></thead><tbody>".format(titles)
    for i, line in enumerate(array):
        if i > limit:
            result += '<tr><td>...</td></tr>'
            break
        result += "<tr>"
        for e in line:
            result += "<td>{}</td>".format(str(e))
        result += "</tr>"
    result += "</tbody></table>"
    logger.log(logging.INFO, result)
    return result


class HiveQLKernel(Kernel):
    implementation = KERNEL_NAME
    implementation_version = __version__
    banner = 'HiveQL REPL'
    language_info = {
        'name': 'hive',
        'codemirror_mode': 'python',
        'pygments_lexer': 'psql',
        'mimetype': 'text/plain',
        'file_extension': '.hiveql',
    }

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if not code.strip():
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }

        # msg_types = https://jupyter-client.readthedocs.io/en/latest/messaging.html?highlight=stream#messages-on-the-iopub-pub-sub-channel
        self.send_response(self.iopub_socket, 'execute_result', {
            "execution_count": self.execution_count,
            'data': {
                # "text/plain" : ["multiline text data"],
                "text/html": array_to_html(*gen_fake_array()),
                # "application/json": {
                # 	"json": "data"
                # }
            },
            "metadata": {
                "image/png": {
                    "width": 640,
                    "height": 480,
                },
            }
        })

        return {
            'status': 'ok',
            'execution_count': self.execution_count,
            'payload': [],
            'user_expressions': {}
        }
