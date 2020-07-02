import requests

class Extractor(object):
    def __init__(self, url):
        self.url = url

    def extract(self):
        return requests.get(self.url)
