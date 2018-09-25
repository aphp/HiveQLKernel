from setuptools import setup, find_packages
from os import path


here = path.abspath(path.dirname(__file__))


with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

with open(path.join(here, 'README.md')) as f:
    long_description = f.read()


from hiveql.constants import __version__, KERNEL_NAME, DISPLAY_NAME

setup(
    name=KERNEL_NAME + "Kernel",
    version=__version__,
    description=DISPLAY_NAME + ' Kernel',
    long_description=long_description,
    url='https://github.com/EDS-APHP/HiveQLKernel',
    author='APHP - EDS',
    license='MIT',
    keywords='Hive HiveQL PyHive Kernel Ipykernel',
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        'console_scripts':
            ['jupyter-hiveql = {}.__main__:main'.format(KERNEL_NAME)],
        # TODO
        # 'pygments.lexers':
        #     ['hiveql_with_magic = sparqlkernel.pygments_hiveql:HiveQLLexerMagics']
    },
    include_package_data=False,  # otherwise package_data is not used
    package_data={
        KERNEL_NAME: ['resources/logo-*.png', 'resources/*.css'],
    },
)
