import pandas, logging

from pprint import pprint

class Transform(object):
    def __init__(self, data, keyWord):
        self.data = data
        self.keyWord = keyWord

    def findTablePos(self):
        pandas.read_excel(self.data, index_col=0)
        pprint(pandas)
