import pandas, logging

from datetime import datetime

class Transform(object):
    def __init__(self, db, keyWords):
        self.db = db
        self.keyWords = keyWords
        self.months = {'Jan': 'Jan', 'Fev': 'Feb', 'Mar': 'Mar', 'Abr': 'Apr',
                       'Mai': 'May', 'Jun': 'Jun', 'Jul': 'Jul', 'Ago': 'Aug',
                       'Set': 'Sep', 'Out': 'Oct', 'Nov': 'Nov', 'Dez': 'Dec'}

    def parseSheet(self, df, sheet):
        self.dataDict = {}
        dfsRaw = df.parse(sheet)
        dfs = dfsRaw.dropna(axis=1, how='all')
        for index, row in dfs.iterrows():
            self.buildDocs(row)

    def buildDocs(self, row):
        rowMonths = [month for month in self.months if row.get(month)]
        docs = []
        for month in self.months:
            doc = {}
            doc['uf'] = row['ESTADO']
            doc['unit'] = row['UNIDADE']
            doc['product'] = row['COMBUSTÍVEL']
            doc['year_month'] = datetime.strptime(str(row['ANO']) + self.months[month], '%Y%b')
            doc['volume'] = row[month]
            doc['created_at'] = datetime.now()
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
