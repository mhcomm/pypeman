from setuptools import setup

setup(name='pypeman',
      version='0.1',
      description='Minimalistic but pragmatic ESB/ETL in python',
      long_description="""Log desc to be done""",
      classifiers=[
        'Development Status :: 1 - Alpha',
        'License :: OSI Approved :: Apache licence',
        'Programming Language :: Python :: 3.4',
        'Topic :: Data Processing',
      ],
      keywords='esb etl data processing asyncio http',
      url='https://github.com/mhcomm/pypeman',
      author='Jeremie Pardou',
      author_email='jeremie.pardou@mhcomm.fr',
      license='Apache License',
      packages=['pypeman'],
      scripts=['bin/pypeman'],
      test_suite='nose.collector',
      install_requires=[
          'aiohttp',
      ],
      tests_require=['nose', 'nose-cover3'],
      zip_safe=False)