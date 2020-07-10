# -*- coding: utf-8 -*-
# Python built-in modules
import os, time, json, subprocess, logging, requests
from shutil import copy2

# Project modules
import transform, database

logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')

class Raizen(object):
    def import_configs(self, **context):
        with open("/raizen/config.json", encoding='utf-8') as file:
            return json.load(file)

    def fetch_data(self, **context):
        configs = context['ti'].xcom_pull(task_ids='import_configs')
        # response = requests.get(configs['wrapper']['url'])
        # content = response.content
        for key in configs['wrapper']['keyWords']:
            copy2('/raizen/vendas-combustiveis-m3.xls','/raizen/{0}Sales.xls'.format(key))
            # fileName = '/raizen/{0}Sales.xls'.format(key)
            # with open(fileName, 'wb') as file:
            #     file.write(content)
        os.remove('/raizen/vendas-combustiveis-m3.xls')

    def convert_data(self, **context):
        subprocess.call(['soffice --headless --convert-to ods /raizen/*.xls'],shell=True)

    def transform_data(self, **context):
        configs = context['ti'].xcom_pull(task_ids='import_configs')
        trs = transform.Transform(configs['wrapper']['keyWords'])
        return trs.run()

    def load_data(self, **context):
        configs = context['ti'].xcom_pull(task_ids='import_configs')
        db = database.Database(configs)
        db.createDB()
        data = context['ti'].xcom_pull(task_ids='transform_data')
        columns = ['uf', 'unit', 'product', 'year_month', 'volume', 'created_at']
        for key in configs['wrapper']['keyWords']:
            db.insert(key, columns, tuple(data[key]))
            db.index(key,'year_month')
