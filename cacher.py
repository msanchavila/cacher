from pathlib import Path
import pandas as pd


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
            # store inputs
            self._filepath = Path(filepath)
            self._cache_dir = Path(cache_dir)
            self._use_cache = use_cache

            # checks before caching            
            self._input_arg_checks()

            # define where data cache will live
            cache_file = self._get_cache_file()

            # caching is on OR cache does not exist OR cache is out dated
            if self._caching_required(cache_file):
                # access target
                target = self._get_reader_target()
                
                # read data from file
                data = self._reader(target, **kwargs)

                # cache to parquet
                data.to_parquet(cache_file, compression='snappy')

            else: # cache file is ready
                # read from cache
                data = pd.read_parquet(cache_file)

            return data

        return wrapper

    def _get_reader_target(self):
        '''Method to use _helper if required

        The purpose of this is to open _filepath which contains a query or execution
        commands used by _reader function.

        If not _helper is defined, _filepath is target
        '''
        return self._helper(self._filepath) if self._helper else self._filepath

    def _get_cache_file(self):
        '''Method to define cache file'''
        return self._cache_dir / '{}.parquet'.format(self._filepath.stem)

    def _caching_required(self, cache_file):
        '''Method to check if caching is required

        Caching is requied when any of these conditions are True;
            - use_cache option is False
            - cache_file does not exists in cache directory
            - target file was modified after cache_file was generated
        '''
        return not self._use_cache or not cache_file.exists() or \
                self._filepath.stat().st_mtime > cache_file.stat().st_mtime

    def _input_arg_checks(self):
        '''Method to check input arguments before beginning cache process'''        
        if not isinstance(self._use_cache, bool):
            raise ValueError('Given use_cache is not type bool')

        self._io_checks()

        self._filetype_check()

    def _filetype_check(self):
        '''Method to check if _filepath is the correct filetype'''
        # check to make sure correct file ext is being
        if self._suffixes and not self._filepath.suffix in self._suffixes:
            raise ValueError('Given filepath is not the correct file extension %s' % 
                self._filepath)

    def _io_checks(self):
        '''Method to execute necessary checks before caching

        Checks to ensure given target file exists, given target file has correct
        file type, and given cache directory exists.
        '''
        # check filepath does exist
        if not self._filepath.is_file():
            raise IOError('Given filepath cannot be found or does not exist %s' %
                self._filepath)

        # check cachedir directory exists
        if not self._cache_dir.is_dir():
            raise IOError('Given cache_dir cannot be found or does not exist %s' %
                self._cache_dir)

@CacheFile(pd.read_json, suffixes = ['.json'])
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

@CacheFile(pd.read_csv, suffixes = ['.csv'])
def cache_csv(filepath, cache_dir, use_cache, **kwargs):
    '''Function to cache csv file

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

@CacheFile(pd.read_excel, suffixes = ['.xlsx', '.xls'])
def cache_excel(filepath, cache_dir, use_cache, **kwargs):
    '''Function to cache excel file

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