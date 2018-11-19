from setuptools import setup

DESCRIPTION = '''
    A tool to cache Excel (.xlsx, .xls), CSV (.csv), JSON (.json) and SQL (.sql) 
    queries as parquets for future reads
'''

setup(
   name='cacher',
   version='1.0',
   description=DESCRIPTION,
   packages=['cacher'], 
   install_requires=['pandas', 'pyarrow', 'xlrd', 'fastparquet'], 
)