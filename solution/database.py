import sqlite3, logging

class Database(object):
    def __init__(self, configs):
        self.configs = configs
        self.cfgDB = self.configs['database']
        self.cfgKeyWords = self.configs['wrapper']['keyWords']

    def createDB(self):
        dbName = self.cfgDB['name']
        self.conn = sqlite3.connect(dbName + '.db')
        logging.info('started DB')
        self.createTables()

    def createTables(self):
        schema = self.cfgDB['defaultSchema']
        for key in self.cfgKeyWords:
            createQuery = "CREATE TABLE IF NOT EXISTS {0} ({1})"
            columns = []
            for column, type in schema.items():
                columns.append(column + ' ' + type)
            createQuery = createQuery.format(key,','.join(columns))
            self.conn.execute(createQuery)
            logging.info('table {0} created'.format(key))

    def select(self, table, fields=None):
        selectQuery = 'SELECT * FROM {0}'.format(table)
        cursor = self.conn.cursor()
        cursor.execute(selectQuery)
        response = cursor.fetchall()
        cursor.close()
        return response

    def insert(self, table, fields, values):
        insertQuery = 'INSERT INTO {0} ({1}) VALUES ({2})'
        insertQuery = insertQuery.format(table,','.join(fields),','.join(len(fields)*'?'))
        cursor = self.conn.cursor()
        cursor.executemany(insertQuery, values)
        self.conn.commit()
        cursor.close()

    def truncate(self, table):
        truncateQuery = 'DELETE FROM {0}'.format(table)
        cursor = self.conn.cursor()
        cursor.execute(truncateQuery)
        logging.info('data truncated from {0}'.format(table))
        self.conn.commit()
        cursor.close()
