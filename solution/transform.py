import pandas, logging

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
            self.buildDocs(row)

    def buildDocs(self, row):
        rowMonths = [month for month in self.months if row.get(month)]
        docs = []
        for month in rowMonths:
            doc = {}
            doc['uf'] = row['ESTADO']
            doc['unit'] = row['UNIDADE']
            doc['product'] = row['COMBUSTÍVEL']
            doc['year_month'] = str(row['ANO']) + '_' + month
            doc['volume'] = row[month]
            doc['created_at'] = datetime.strftime(datetime.now(), '%d/%m/%Y %H:%M:%S')
            docs.append(list(doc.values()))
        keys = ['uf', 'unit', 'product', 'year_month', 'volume', 'created_at']
        self.db.insert(self.currentKey, keys, tuple(docs))

    def run(self):
        for key in self.keyWords:
            logging.info('started parsing and loading {0} sheet'.format(key))
            self.currentKey = key
            df = pandas.ExcelFile('files/{0}Sales.xls'.format(key))
            sheets = df.sheet_names
            for sheet in sheets:
                parsed = self.parseSheet(df, sheet)
