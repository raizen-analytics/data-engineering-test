import sys, math, pandas, logging

from pprint import pprint
from datetime import datetime

class Transform(object):
    def __init__(self, db, keyWords):
        self.db = db
        self.keyWords = keyWords
        self.months = ['Jan', 'Fev','Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']

    def parseSheet(self, df, sheet):
        self.dataDict = {}
        dfsRaw = df.parse(sheet)
        dfs = dfsRaw.dropna(axis=1, how='all')
        for index, row in dfs.iterrows():
            uf = row['ESTADO']
            if not uf in self.dataDict:
                self.dataDict[uf] = {}
            self.aggregateMonth(uf, row)
        self.loadData()

    def aggregateMonth(self, uf, row):
        rowMonths = [month for month in self.months if row.get(month)]
        for month in rowMonths:
            self.dataDict[uf]['unit'] = row['UNIDADE']
            self.dataDict[uf]['product'] = row['COMBUSTÍVEL']
            self.dataDict[uf]['year_month'] = str(row['ANO']) + '_' + month
            self.dataDict[uf]['volume'] = row[month]
            if math.isnan(row[month]):
                self.dataDict[uf]['volume'] = 0
            self.dataDict[uf]['created_at'] = datetime.now()

    def loadData(self):
        pprint(self.dataDict)
        del self.dataDict

    def run(self):
        for key in self.keyWords:
            df = pandas.ExcelFile('files/{0}Sales.xls'.format(key))
            sheets = df.sheet_names
            for sheet in sheets:
                parsed = self.parseSheet(df, sheet)
