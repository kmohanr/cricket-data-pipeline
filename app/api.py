import requests

def fetch_data(url):

    response = requests.get(url)

    if response.status_code == 200:
        return response.json()

    return None
