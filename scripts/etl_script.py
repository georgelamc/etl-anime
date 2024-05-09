import psycopg2
import requests

API_URL = 'https://kitsu.io/api/edge'


def get_connection():
    connection = psycopg2.connect(
        host='etl_anime_database',
        port='5432',
        dbname='database',
        user='user',
        password='password'
    )

    return connection


def create_table():
    statement = (
        'create table anime('
        'id serial primary key,'
        'title varchar(255),'
        'startDate date,'
        'endDate date,'
        'episodes integer'
        'rating decimal)'
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

    num_gets = 0  # for testing

    while response is not None and num_gets < 2:
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

        num_gets += 1

    return data


create_table()
insert(get_anime())
