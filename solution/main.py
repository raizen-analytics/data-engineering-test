# -*- coding: utf-8 -*-

### Import Section
# Python built-in modules
import os, json, sys, sqlite3

# Project modules
import transform, database

# 3rd part modules
import logging, requests, xlwings

### Code Section
logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')

class Raizen(object):
    def __init__(self):
        logging.info('app started')
        self.configs = self.importConfigs()
        self.url = self.configs['wrapper']['url']
        self.keyWords = self.configs['wrapper']['keyWords']

    def importConfigs(self):
        with open("config.json", encoding='utf-8') as file:
            return json.load(file)
        logging.info('configs imported')

    def downloadSheet(self):
        logging.info('sheet downloaded started')
        response = requests.get(self.url)
        content = response.content
        logging.info('sheet downloaded finished')
        for key in self.keyWords:
            with open('files/{0}Sales.xls'.format(key), 'wb') as file:
                file.write(content)
            logging.info('{0} created'.format(fileName))

    def getPivotSourceData(self):
        logging.info('pivot source data extraction started')
        app = xlwings.App(visible=False)
        app.display_alerts = False
        vba = xlwings.Book('files/vbas.xlsm')
        for key in self.keyWords:
            path = 'files/{0}Sales.xls'.format(key)
            file = xlwings.Book(path)
            runVBA = vba.macro('{0}PivotSourceData'.format(key))
            runVBA()
            file.save()

        apps = xlwings.apps.active
        apps.quit()
        logging.info('pivot source data extraction finished')

    def startDB(self):
        db = database.Database(self.configs)
        db.createDB()
        db.insert('oil')
        db.truncate('oil')
        db.select('oil')

    def prepareData(self):
        trs = transform.Transform(data,self.keyWords)
        trs.getPivotTables()

    def run(self):
        # self.downloadSheet()
        # self.getPivotSourceData()
        # self.startDB()
        self.prepareData()
        # trs.show()
        logging.info('app finished')

# if __file__ == "__main__":
r = Raizen()
r.run()
