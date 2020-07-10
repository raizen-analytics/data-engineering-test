import json, math, pandas, logging

from datetime import datetime

class Transform(object):
    def __init__(self, keyWords):
        self.allDocs = {}
        self.keyWords = keyWords
        self.months = {'Jan': 'Jan', 'Fev': 'Feb', 'Mar': 'Mar', 'Abr': 'Apr',
                       'Mai': 'May', 'Jun': 'Jun', 'Jul': 'Jul', 'Ago': 'Aug',
                       'Set': 'Sep', 'Out': 'Oct', 'Nov': 'Nov', 'Dez': 'Dec'}

    def parseDf(self, df):
        dfs = pandas.DataFrame(df)
        dfs = dfs.dropna(axis=1, how='all')
        for row in dfs.itertuples():
            self.buildDocs(row)

    def buildDocs(self, row):
        for month in self.months:
            doc = {}
            doc['uf'] = getattr(row, 'ESTADO')
            doc['unit'] = 'm3'
            doc['product'] = getattr(row, 'COMBUSTÍVEL')
            doc['year_month'] = datetime.strptime(str(getattr(row, 'ANO')) + self.months[month], '%Y%b')
            doc['volume'] = getattr(row, month)
            if math.isnan(getattr(row, month)):
                doc['volume'] = 0
            doc['created_at'] = datetime.now()
            self.allDocs[self.currentKey].append(list(doc.values()))

    def run(self):
        for key in self.keyWords:
            logging.info('parsing {0} sheet'.format(key))
            self.allDocs[key] = []
            self.currentKey = key
            fileName = '/usr/local/airflow/{0}Sales.ods'.format(key)
            df = pandas.read_excel(fileName, sheet_name='DPCache_m3', engine='odf')
            self.parseDf(df)
        return self.allDocs
