import psycopg2
import requests
import subprocess
import time

API_URL = 'https://kitsu.io/api/edge'


def get_connection():
    connection = psycopg2.connect(
        host='database',
        port='5432',
        dbname='database',
        user='user',
        password='password'
    )
    return connection


def wait_for_database():
    tries = 0
    max_retries = 5
    sleep_time = 5
    while tries < max_retries:
        try:
            result = subprocess.run(['pg_isready', '-h', 'database'])
            if result.returncode == 0:
                return
        except Exception as e:
            print(f'Error: {e}')
        tries += 1
        time.sleep(sleep_time)


def create_tables():
    statement = (
        'create table anime('
        'id serial primary key, '
        'title varchar(255), '
        'startDate date, '
        'endDate date, '
        'episodes integer, '
        'rating decimal'
        ')'
    )
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute(statement)


def insert(data):
    connection = get_connection()
    cursor = connection.cursor()
    for d in data:
        attributes = d['attributes']
        title = attributes['titles']['en']
        start_date = attributes['startDate']
        end_date = attributes['endDate']
        episodes = attributes['episodeCount']
        rating = attributes['averageRating']
        statement = (f'insert into anime(title, startDate, endDate, episodes, rating) '
                     f'values("{title}", {start_date}, {end_date}, {episodes}, {rating})')
        cursor.execute(statement)


def get_anime():
    data = []
    url = f'{API_URL}/anime'
    response = requests.get(url)
    while response is not None:
        if response.status_code != 200:
            print(f'Error: Status code: {response.status_code}.')
            exit(1)
        elif 'data' not in response.json():
            print('Error: There is no data in the response.')
            exit(1)
        for value in response.json()['data']:
            data.append(value)
        if 'links' in response.json():
            links = response.json()['links']
            if 'next' in links:
                url = links['next']
                response = requests.get(url)
    return data


wait_for_database()
create_tables()
insert(get_anime())
