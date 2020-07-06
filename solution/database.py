import sqlite3

class Database(object):
    def __init__(self, configs):
        self.configs = configs
        self.cfgDB = self.configs['database']
        self.cfgKeyWords = self.configs['wrapper']['keyWords']

    def createDB(self):
        dbName = self.cfgDB['name']
        self.conn = sqlite3.connect(dbName + '.db')
        self.createTables()

    def createTables(self):
        schema = self.cfgDB['deafultSchema']
        for key in self.cfgKeyWords:
            createQuery = "CREATE TABLE IF NOT EXISTS {0} ({1})"
            columns = []
            for column, type in schema.items():
                columns.append(column + ' ' + type)
            createQuery = createQuery.format(key,','.join(columns))
            self.conn.execute(createQuery)

    def select(self, table, fields=None):
        selectQuery = 'SELECT * FROM {0}'.format(table)
        cursor = self.conn.cursor()
        cursor.execute(selectQuery)
        response = cursor.fetchall()
        cursor.close()
        return response

    def insert(self, table, values=None):
        insertQuery = 'INSERT INTO {0} VALUES ({1})'
        values = ['\'2020_Janeiro\'','\'SP\'','\'OLEO\'','\'m3\'','\'1000\'','\'2020\'']
        insertQuery = insertQuery.format(table,','.join(values))
        cursor = self.conn.cursor()
        cursor.execute(insertQuery)
        self.conn.commit()
        cursor.close()

    def truncate(self, table):
        truncateQuery = 'DELETE FROM {0}'.format(table)
        cursor = self.conn.cursor()
        cursor.execute(truncateQuery)
        self.conn.commit()
        cursor.close()
