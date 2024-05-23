import psycopg2
import requests
import subprocess
import sys
import time

API_URL = 'https://kitsu.io/api/edge'
DATE_FORMAT = '%Y-%m-%d'
SOURCE_DATABASE_CONFIG = {
    'host': 'source_database_service',
    'port': '5432',
    'dbname': 'database',
    'user': 'user',
    'password': 'password'
}
DESTINATION_DATABASE_CONFIG = {
    'host': 'destination_database_service',
    'port': '5432',
    'dbname': 'database',
    'user': 'user',
    'password': 'password'
}


def get_source_connection():
    connection = psycopg2.connect(
        host=SOURCE_DATABASE_CONFIG['host'],
        port=SOURCE_DATABASE_CONFIG['port'],
        dbname=SOURCE_DATABASE_CONFIG['dbname'],
        user=SOURCE_DATABASE_CONFIG['user'],
        password=SOURCE_DATABASE_CONFIG['password'])
    return connection


def get_destination_connection():
    connection = psycopg2.connect(
        host=DESTINATION_DATABASE_CONFIG['host'],
        port=DESTINATION_DATABASE_CONFIG['port'],
        dbname=DESTINATION_DATABASE_CONFIG['dbname'],
        user=DESTINATION_DATABASE_CONFIG['user'],
        password=DESTINATION_DATABASE_CONFIG['password'])
    return connection


def connect_to_database(database, max_retries, sleep_time):
    tries = 0
    while tries < max_retries:
        try:
            result = subprocess.run(['pg_isready', '-h', database], capture_output=True, check=True)
            if result.returncode == 0:
                return True
        except subprocess.CalledProcessError as e:
            print(f'Error: {e.stderr}')
            print('Trying again...')
        tries += 1
        time.sleep(sleep_time)
    return False


def is_database_empty():
    connection = get_source_connection()
    cursor = connection.cursor()
    statement = 'select count(*) from anime'
    cursor.execute(statement)
    return cursor.fetchone()[0] == 0


def insert(data):
    connection = get_source_connection()
    cursor = connection.cursor()
    for d in data:
        attributes = d['attributes']
        titles = attributes['titles']
        start_date = attributes['startDate']
        end_date = attributes['endDate']
        episodes = -1 if attributes['episodeCount'] is None else attributes['episodeCount']
        rating = -1 if attributes['averageRating'] is None else attributes['averageRating']
        title = None
        if 'en' in titles:
            title = titles['en']
        elif 'en-jp' in titles:
            title = titles['en-jp']
        if title is None:
            continue
        statement = (f'insert into anime(title, startDate, endDate, episodes, rating) '
                     f'values(%s, to_date(%s, %s), to_date(%s, %s), {episodes}, {rating});')
        cursor.execute(statement, (title, start_date, DATE_FORMAT, end_date, DATE_FORMAT))
    connection.commit()
    connection.close()


def get_anime(max_pages=sys.maxsize):
    data = []
    url = f'{API_URL}/anime'
    is_done = False
    page = 0
    while not is_done:
        response = requests.get(url)
        if response.status_code != 200:
            print(f'Error: Status code: {response.status_code}.')
            exit(1)
        elif 'data' not in response.json():
            print('Error: There is no data in the response.')
            exit(1)
        for value in response.json()['data']:
            data.append(value)
        if 'links' in response.json() and 'next' in response.json()['links']:
            links = response.json()['links']
            url = links['next']
        else:
            is_done = True
        page += 1
        if page == max_pages:
            is_done = True
    return data


def extract():
    dump_command = [
        'pg_dump',
        '-h', SOURCE_DATABASE_CONFIG['host'],
        '-U', SOURCE_DATABASE_CONFIG['user'],
        '-d', SOURCE_DATABASE_CONFIG['dbname'],
        '-f', 'data_dump.sql',
        '-w'
    ]
    try:
        subprocess_env = dict(PGPASSWORD=SOURCE_DATABASE_CONFIG['password'])
        subprocess.run(dump_command, capture_output=True, check=True, env=subprocess_env, text=True)
    except subprocess.CalledProcessError as e:
        print(f'Error: {e.stderr}')
        exit(1)


def load():
    load_command = [
        'psql',
        '-h', DESTINATION_DATABASE_CONFIG['host'],
        '-U', DESTINATION_DATABASE_CONFIG['user'],
        '-d', DESTINATION_DATABASE_CONFIG['dbname'],
        '-f', 'data_dump.sql',
        '-a'
    ]
    try:
        subprocess_env = dict(PGPASSWORD=SOURCE_DATABASE_CONFIG['password'])
        subprocess.run(load_command, capture_output=True, check=True, env=subprocess_env, text=True)
    except subprocess.CalledProcessError as e:
        print(f'Error: {e.stderr}')
        exit(1)


print('Connecting to database...')
if not connect_to_database(SOURCE_DATABASE_CONFIG['host'], 5, 5):
    print('Error connecting to the source database.')
    exit(1)
if not connect_to_database(DESTINATION_DATABASE_CONFIG['host'], 5, 5):
    print('Error connecting to the destination database.')
    exit(1)
print('Database connection established.')
print('Checking data...')
if is_database_empty():
    print('Database is empty.')
    print('Downloading data...')
    insert(get_anime(1))
    print('Download complete.')
else:
    print('Database has data.')
print('Extracting data...')
extract()
print('Extraction complete.')
print('Loading data...')
load()
print('Load complete.')
