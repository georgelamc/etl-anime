import requests

API_ENDPOINT_ANIME = '/anime'
API_URL = 'https://kitsu.io/api/edge'


def get(url):
    response = requests.get(url)
    if response.status_code == 200 and 'data' in response.json():
        data = response.json()['data']
        for key in data:
            print(key)
        if 'links' in response.json():
            links = response.json()['links']
            if 'next' in links:
                endpoint = links['next'].split
                get(links['next'])
    else:
        print(f'Error: {response.status_code}')
        exit(1)


headers = {
    'Accept': 'application/vnd.api+json',
    'Content-Type': 'application/vnd.api+json'
}

get(API_URL + API_ENDPOINT_ANIME)
