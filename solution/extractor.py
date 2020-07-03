import requests

class Extractor(object):
    def __init__(self, url):
        self.url = url

    def extract(self):
        response = requests.get(self.url)
        return response.content
