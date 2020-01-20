import os
__version__ = '1.0.16'

KERNEL_NAME = 'hiveql'
LANGUAGE = 'hiveql'
DISPLAY_NAME = 'HiveQL'

DEFAULT_TEXT_LANG = ['en']

CONFIG_FILE = os.environ.get("HIVE_KERNEL_CONF_FILE", "~/.hiveql_kernel.json")
