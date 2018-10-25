#!/usr/bin/env python
'''
    Parquet caching functionality
    Written by Martin Sanchez (https://github.com/msanchavila)
    Inspired by Alex Chen (alchenist)
'''
from pathlib import Path
import pandas as pd


def format_cache_file(cache_dir, filename):
    '''Function to format cache file name and location

    Args:
        cache_dir <str>: Directory to store cache
        filename <str>: File name

    Return <str>:
        Location of file 
    '''
    return cache_dir / '{}.parquet'.format(filename)

def caching_required(filepath, cache_file, use_cache):
    '''Function to check if caching will be required

        Caching is requied when any of these conditions are True;
        - use_cache option is False
        - cache_file does not exist in cache directory
        - target file was modified after cache_file was generated

    Args:
        filepath <str>: location of souce data file
        cache_file <str>: location of target cache file
        use_cache <bool>: 

    Return <bool>:
        Indicate if caching is required
    '''
    return not use_cache or not cache_file.exists() or \
            filepath.stat().st_mtime > cache_file.stat().st_mtime


class CacheFile(object):
    '''Decorator to add caching to functions that read files into dataframes

    This will create a parquet version of given data. It will continue to use
    the parquet until an update in the oringal file is detected, then it will re-
    create the parquet.

    Args:
        reader <func>: Function to read data from file
        suffixes <tuple,list>: Acceptable file extensions
        helper <func>: helper function to access query/command in file
            Ex. filepath.read() to get sql query

    Raises:
        IOError:
            - filepath is not a file or does not exist
            - cache_dir is not a directory or does not exist

        ValueError:
            - filepath is not the correct extension
            - use_cache is not bool

    Return:
        Pandas.DataFrame
    '''

    def __init__(self, reader=None, suffixes=None, helper=None):
        self._reader = reader
        self._helper = helper
        self._suffixes = suffixes

    def __call__(self, func):

        def wrapper(filepath, cache_dir, use_cache=True, **kwargs):
            '''Method to wrap cache function

            Does not call wrapped func, instead uses _reader; given as decorator
            parameter
            '''
            # convert to Path objects
            filepath = Path(filepath)
            cache_dir = Path(cache_dir)

            # checks before caching
            self._check_input_args(filepath, cache_dir, use_cache)

            # define where data cache will live
            cache_file = format_cache_file(cache_dir, filepath.stem)

            # caching is on OR cache does not exist OR cache is out dated
            if caching_required(filepath, cache_file, use_cache):
                # access target
                target = self._get_reader_target(filepath)

                # read data from file
                data = self._reader(target, **kwargs)

                # cache to parquet
                data.to_parquet(cache_file, compression='snappy')

            else: # cache file is ready
                # read from cache
                data = pd.read_parquet(cache_file)

            return data

        return wrapper

    def _get_reader_target(self, filepath):
        '''Method to use _helper if required

        The purpose of this is to open _filepath which contains a query or execution
        commands used by _reader function.

        If _helper is not defined, _filepath is target
        '''
        return self._helper(filepath) if self._helper else filepath

    def _check_input_args(self, filepath, cache_dir, use_cache):
        '''Method to execute necessary checks before caching

        Checks to ensure given target file exists, given target file has correct
        file type, and given cache directory exists.
        '''
        if not isinstance(use_cache, bool):
            raise ValueError('Given use_cache is not type bool')

        # check filepath does exist
        if not filepath.is_file():
            raise IOError('Given filepath cannot be found or does not exist %s' %
                          filepath)

        # check cachedir directory exists
        if not cache_dir.is_dir():
            raise IOError('Given cache_dir cannot be found or does not exist %s' %
                          cache_dir)

        # check to make sure correct file ext is being
        if self._suffixes and not filepath.suffix in self._suffixes:
            raise ValueError('Given filepath is not the correct file extension %s' %
                             filepath)


@CacheFile(pd.read_json, suffixes=['.json'])
def cache_json(filepath, cache_dir, use_cache, **kwargs):
    '''Function to cache json file or valid JSON string

    Will ensure an updated version of filepath is cached as a parquet file in
    cache_dir. The parquet file will have the same name as filepath.

    some_data.json -> ~/cache_dir/some_data.parquet

    Args:
        filepath <str>: json file of valid JSON string to cache
        cache_dir <str>: dirpath to find and store cache
        use_cache <bool>: trigger to use cache if necessary; using False will always
            regenerate cache
            default: True
        **kwargs: arguments to pass to pandas.read_json

    Returns <pd.DataFrame>
    '''
    pass

@CacheFile(pd.read_csv, suffixes=['.csv'])
def cache_csv(filepath, cache_dir, use_cache, **kwargs):
    '''Function to cache CSV file

    Will ensure an updated version of filepath is cached as a parquet file in
    cache_dir. The parquet file will have the same name as filepath.

    some_data.csv -> ~/cache_dir/some_data.parquet

    Args:
        filepath <str>: filepath fo csv to cache
        cache_dir <str>: dirpath to find and store cache
        use_cache <bool>: trigger to use cache if necessary; using False will always
            regenerate cache
            default: True
        **kwargs: arguments to pass to pandas.read_csv

    Returns <pd.DataFrame>
    '''
    pass

@CacheFile(pd.read_excel, suffixes=['.xlsx', '.xls'])
def cache_excel(filepath, cache_dir, use_cache, **kwargs):
    '''Function to cache Excel

    Will ensure an updated version of filepath is cached as a parquet file in
    cache_dir. The parquet file will have the same name as filepath.

    some_data.xlsx -> ~/cache_dir/some_data.parquet

    Args:
        filepath <str>: filepath fo csv to cache
        cache_dir <str>: dirpath to find and store cache
        use_cache <bool>: trigger to use cache if necessary; using False will always
            regenerate cache
            default: True
        **kwargs: arguments to pass to pandas.read_excel

    Returns <pd.DataFrame>
    '''
    pass

@ CacheFile(pd.read_sql_query, suffixes=['.sql'], helper=lambda x: x.read_text())
def cache_sql(filepath, cache_dir, use_cache, **kwargs):
    '''Function to cache SQL query

    Will ensure an updated version of filepath is cached as a parquet file in
    cache_dir. The parquet file will have the same name as filepath.

    some_data.sql -> ~/cache_dir/some_data.parquet

    Args:
        filepath <str>: filepath fo csv to cache
        cache_dir <str>: dirpath to find and store cache
        use_cache <bool>: trigger to use cache if necessary; using False will always
            regenerate cache
            default: True
        con <str>: SQLAlchemy connectable(engine/connection), database string URI
        **kwargs: arguments to pass to pandas.read_sql_query

    Returns <pd.DataFrame>
    '''
    pass
