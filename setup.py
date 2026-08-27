from setuptools import setup
from importlib import import_module

VERSION = import_module("pypeman").__version__
URL = import_module("pypeman").__url__

with open("./README.rst") as desc_file:
    long_description = desc_file.read()

setup(
    name="pypeman",
    version=VERSION,
    description="Minimalistic but pragmatic ESB / ETL / EAI in Python",
    long_description=long_description,
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Healthcare Industry",
        "Operating System :: POSIX :: Linux",
        "Topic :: Database",
        "Topic :: Internet",
        "Topic :: Internet :: File Transfer Protocol (FTP)",
        "Programming Language :: Python",
    ],
    keywords="esb etl eai pipeline data processing asyncio http ftp hl7",
    url=URL,
    author="Jeremie Pardou",
    author_email="jeremie@jeremiez.net",
    license="Apache Software License",
    packages=[
        "pypeman",
        "pypeman.contrib",
        "pypeman.helpers",
        "pypeman.plugins",
        "pypeman.plugins.remoteadmin",
        "pypeman.plugins.tests",
        "pypeman.tests",
        "pypeman.tests.test_app",
        "pypeman.tests.test_app_testing",
        "pypeman.tests.settings",
    ],
    package_data={
        "pypeman.tests": ["data/*"],
    },
    entry_points={
        "console_scripts": [
            "pypeman = pypeman.commands:main",
        ]
    },
    scripts=[
        "bin/pypeman-startproject",
    ],
    install_requires=[
        "python-dateutil",
        "aiohttp",
        "aiosqlite",
        "sqlitedict",
    ],
    extras_require={
        "hl7": ["hl7"],
        "xml": ["xmltodict"],
        "time": ["aiocron"],
        "all": ["hl7", "xmltodict", "aiocron"],
    },
    include_package_data=True,
)
