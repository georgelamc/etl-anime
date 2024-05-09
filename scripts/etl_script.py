import requests

API_URL = 'https://kitsu.io/api/edge'


def get_anime():
    url = f'{API_URL}/anime'
    response = requests.get(url)

    num_gets = 0  # for testing

    while response is not None and num_gets < 1:
        if response.status_code != 200:
            print(f'Error: Status code: {response.status_code}.')
            exit(1)
        elif 'data' not in response.json():
            print('Error: There is no data in the response.')
            exit(1)

        data = response.json()['data']
        for key in data:
            print(key)

        if 'links' in response.json():
            links = response.json()['links']
            if 'next' in links:
                url = links['next']
                response = requests.get(url)

        num_gets += 1


get_anime()
