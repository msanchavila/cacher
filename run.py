import os
from dotenv import find_dotenv, load_dotenv
from cacher import cache_csv, cache_excel, cache_sql


CACHE_DIR  = './data/cache'

def main():
    
    print(cache_csv('./data/raw/csv_data.csv', CACHE_DIR))

    print(cache_excel('./data/raw/excel_data.xlsx', CACHE_DIR))

    print(cache_sql('./data/queries/some_query.sql', CACHE_DIR, 
        con=os.getenv('DATABASE_URI')))


if __name__ == '__main__':
    load_dotenv(find_dotenv())
    main()