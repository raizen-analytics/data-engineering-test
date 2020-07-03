import pandas, logging

from pprint import pprint

class Transform(object):
    def __init__(self, data, keyWords):
        self.data = data
        self.keyWords = keyWords
        self.dfs = []

    def getPivotTables(self):
        sheet = pandas.read_excel(self.data)
        for keyWord in self.keyWords:
            df = sheet.iloc[:,1]
            indexArray = df[sheet.iloc[:,1] == keyWord].index.values
            index = indexArray[0].item()
            df = sheet.iloc[index+9:index+23,1:24]
            df = df.dropna(axis=1, how='all')
            header = df.iloc[0]
            df = df[1:]
            df.columns = header
            self.dfs.append(df)
