import requests, logging

class Extractor(object):
    def __init__(self, url):
        self.url = url

    def extract(self):
        try:
            response = requests.get(self.url)
        except:
            logging.error('extract failed')
            return False

        if response.status_code == 200:
            return response.content
        else:
            logging.error('extract failed')
            return False
