import pandas as pd  #Used to manipulate data in dataframes, could use sparkdf also for larger operations
from openpyxl import load_workbook  #Used to extract the data from pivot tables
from datetime import datetime  #used to create timestamp data in the table
import shutil

#Before working with the folder, it is needed to convert file to xlxs file format
#This can be done manually or by running a headless libreOffice app
def load_worksheet(workbook, worksheet):
    #Function used to fetch our worksheet
    #param - workbook: str - must use excel file path and name
    #param - worksheet: str - worksheet name
    workbook = load_workbook(workbook)
    return workbook[worksheet]

#After obtaining all table definitions, we'll fetch the cacheDefinitions to work with,
#instead of reading every CacheDefinition.xml file.
#This function will then return a DataFrame with the table records and its dimensions
def data_extract(cache_data):

    rows = []
    dims = {}
    for cf in cache_data.cacheFields:
        dims[cf.name] = cf.name

    for dat in cache_data.records.r:
        data = []
        for cols in rows._fields:
            try:  #Used to handle missing data
                data.append(cols.v)
            except :
                data.append(None)
        rows.append(data)
    #We will now create a DataFrame to work with in our ETL process.
    df = pd.DataFrame(columns=dims, data=rows)
    return df
 
#This function will now create the table as requested by the user.
#1-This will first change the table format to encompass months as a single column rather than multiple.
#2-After shifting the table format, we will alter some of the column names and drop ones we dont use.
#3-We will then deal with null values
#4-After altering the columns we will change month names in order to create a 'year_month' column
#5-This will also deal with some of the issues in 'ANO' in which there was some float numbers
#6-And finally year_month will be transformet into dateformat and drop the now unused 'mes' column.
def transform_data(table):
    transformed_df = pd.melt(table,
                            id_vars=['COMBUSTÍVEL', 'REGIÃO', 'ANO', 'ESTADO', 'UNIDADE', 'TOTAL'],
                            var_name='mes',
                            value_name='volume') 
    transformed_df['mes'] = transformed_df['mes'].replace({
                                                                'Jan': '01',
                                                                'Fev': '02',
                                                                'Mar': '03',
                                                                'Abr': '04',
                                                                'Mai': '05',
                                                                'Jun': '06',
                                                                'Jul': '07',
                                                                'Ago': '08',
                                                                'Set': '09',
                                                                'Out': '10',
                                                                'Nov': '11',
                                                                'Dez': '12'})
    
    transformed_df = transformed_df.rename(columns={
                                                    "ESTADO": "uf",
                                                    "ANO": "year_month",
                                                    "UNIDADE": "unit",
                                                    "COMBUSTÍVEL": "product"})
                                            
    transformed_df = transformed_df.fillna(0)

    transformed_df.drop(['TOTAL','REGIÃO'], axis=1, inplace=True)

    transformed_df = transformed_df.astype({"year_month":int})

    transformed_df = transformed_df.astype({"year_month":str})

    transformed_df['year_month'] = transformed_df['year_month'] + transformed_df['mes']

    transformed_df['year_month'] = pd.to_datetime(transformed_df['year_month'], format="%Y%m")

    transformed_df.drop(['mes'])

    transformed_df['created_at'] = pd.to_datetime('today')


