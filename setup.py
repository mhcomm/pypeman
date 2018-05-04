from setuptools import setup
from importlib import import_module

VERSION = import_module("pypeman").__version__
URL = import_module("pypeman").__url__

with open("./README.rst") as desc_file:
    long_description = desc_file.read()

setup(
    name='pypeman',
    version=VERSION,
    description='Minimalistic but pragmatic ESB / ETL / EAI in Python',
    long_description=long_description,
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3 :: Only',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Healthcare Industry',
        'Operating System :: POSIX :: Linux',
        'Topic :: Database',
        'Topic :: Internet',
        'Topic :: Internet :: File Transfer Protocol (FTP)',
        'Programming Language :: Python',
    ],
    keywords='esb etl eai pipeline data processing asyncio http ftp hl7',
    url=URL,

    author='Jeremie Pardou',
    author_email='jeremie@jeremiez.net',

    license='Apache Software License',
    packages=['pypeman', 'pypeman.helpers', 'pypeman.contrib'],

    entry_points={
        'console_scripts': [
            'pypeman = pypeman.commands:run.start',
        ]
    },

    test_suite='nose.collector',
    install_requires=[
        'begins',
        'daemonlite',
        'websockets',
        'aiohttp',
        'jsonrpcclient[websockets]',
        'jsonrpcserver',
        'sqlitedict',
        # TODO Remove next dependency as soon as jsonrpcclient is removed
        'requests'
    ],
    extras_require={
        'hl7': ["hl7"],
        'xml': ["xmltodict"],
        'time': ["aiocron"],
        'all': ["hl7", "xmltodict", "aiocron"]
    },
    setup_requires=['pytest-runner'],
    tests_require=[
        'pytest', 'pytest-cov', 'pytest-asyncio'
        ],
    include_package_data=True,
)
