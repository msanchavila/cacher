# Caching

Caching is a module with functionality to cache large datasets or long-running queries for future re-use and faster reads. 

Simply stated, its a thin wrapper around some of Pandas read calls which stores the resultant DataFrame as a Parquet. 

## Why Cache?

A reasonable question to ask is "If my data is already in a file, why should I consider re-storing my data in a 
different file format?"

The underlying technology used to cache your data is [Apache Parquet](https://parquet.apache.org/). Parquets follow the
_**Write Once Read Many**_ (WORM) paradigm; which makes them exceptionally fast to read but slow to write. This trade off
is great if you have a large file or query you want to constantly re-read for analysis and visualization. Instead of
reading the large file/query every time, you can read it once, write a parquet and re-use the parquet; saving valuable time and
storage space in the process. 

### Cachers

Currently there are 4 file types supported, each with its corresponding cacher. 
Under the hood, these cachers are powered by an equivalent Pandas read function. 
You can make full use of Pandas' read functions by passing in keyword arguments into the cacher. 
_Please refer to Pandas documentation for these keyword arguments._ 

* Excel (*.xlsx, *.xls) -> **cache_excel** -> [Pandas.read_excel](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_excel.html)
* CSV (*.csv) -> **cache_csv** -> [Pandas.read_csv](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html)
* JSON (*.json) -> **cache_json** -> [Pandas.read_json](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_json.html)
* SQL (*.sql) -> **cache_sql** -> [Pandas.read_sql](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql_query.html)


## Usage

The snippet below demostrates the basic usage of each cacher.

```
from cacher import cache_csv, cache_excel, cache_json, cache_sql

CACHE_DIR  = './cache'

def main():
    df_csv = cache_csv('some_data.csv', CACHE_DIR)

    df_xlsx = cache_excel('excel_data.xlsx', CACHE_DIR)

    df_json = cache_json('json_data.json', CACHE_DIR)

    df_sql = cache_sql('sql_query.sql', CACHE_DIR, con='your:connection:string')


if __name__ == '__main__':
    main()
```

### Caching

So, how do these cachers _**cache**_? 

When a file/query is initially given, the Pandas read function is invoked and a DataFrame is received. The resultant 
DataFrame is materialized as your cache. 

**Note: The cache file re-uses the file/query filename as its own**

` data_file.csv --> CACHE/DIR/data_file.parquet`

Later on, when you decide to re-read your data, instead of re-executing your expensive query or re-reading your large file,
the cacher looks into your cache directory and quickly reads the cache instead.

There are certain conditions where the cacher will need to re-create your cache: 

1. `use_cache` flag is set to `False`
2. cache does not exist in cache directory
3. Raw file/query was updated after the parquet file was created

If either of those conditions are `True`, the cacher will attempt to re-create your cache. 

# Authors
    * [Alex Chen](https://github.com/alchenist)
    * [Martin Sanchez](https://github.com/msanchavila)
