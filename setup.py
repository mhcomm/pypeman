from setuptools import setup
from importlib import import_module

VERSION = import_module("pypeman").__version__
URL = import_module("pypeman").__url__

setup(name='pypeman',
      version=VERSION,
      description='Minimalistic but pragmatic ESB / ETL / EAI in Python',
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.4',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Healthcare Industry',
        'Operating System :: POSIX :: Linux',
        'Topic :: Database',
        'Topic :: Internet',
        'Topic :: Internet :: File Transfer Protocol (FTP)',
        'Programming Language :: Python',
      ],
      keywords='esb etl eai pipeline data processing asyncio http',
      url=URL,

      author='Jeremie Pardou',
      author_email='jeremie.pardou@mhcomm.fr',

      license='Apache Software License',
      packages=['pypeman', 'pypeman.helpers', 'pypeman.tst_helpers'],
      entry_points = { 'console_scripts': [
            'pypeman = pypeman.commands:run.start',
        ]},

      test_suite='nose.collector',
      install_requires=['begins'],
      tests_require=['nose', 'nose-cover3']
)

