import pandas, logging

from pprint import pprint

class Transform(object):
    def __init__(self, data, keyWords):
        self.data = data
        self.keyWords = keyWords
        self.startColPos = 1
        self.endColPos = 24
        self.startRowPos = 9
        self.endRowPos = 23
        self.dfs = []

    def getPivotTables(self):
        sheet = pandas.read_excel(self.data)
        for keyWord in self.keyWords:
            df = sheet.iloc[:,1]
            try:
                indexArray = df[sheet.iloc[:,1] == keyWord].index.values
                index = indexArray[0].item()
                self.selectPivotRange(sheet, index)
            except:
                logging.info('keyword: {0} not found in the database'.format(keyWord))

    def selectPivotRange(self, sheet, index):
        df = sheet.iloc[index+self.startRowPos:index+self.endRowPos,
                                    self.startColPos:self.endColPos]
        df = df.dropna(axis=1, how='all')
        header = df.iloc[0]
        df = df[1:]
        df.columns = header
        self.dfs.append(df)
