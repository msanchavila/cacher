from dotenv import find_dotenv, load_dotenv
from cacher import cache_csv, cache_excel, cache_sql


def main():
    print(cache_csv('./data/raw/some_data.csv', './data/raw/cache'))

    print(cache_excel('./data/raw/excel_data.xlsx', './data/raw/cache'))

    print(cache_sql('./data/raw/queries/some_query.sql', './data/raw/cache',
        con='connection_string'))


if __name__ == '__main__':
    from dotenv import find_dotenv, load_dotenv
    main()