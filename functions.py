import pandas as pd  # to work with data
from openpyxl import load_workbook  # to work with excel xml
from datetime import datetime  # to timestamp stuff
import numpy as np
import shutil

# After saving in xlsx format, we can now work on the xml files with openpyxl
def load_worksheet(workbook, worksheet):
    '''
    Generic function that returns a worksheet object
    :param workbook: str containing the workbook name and path
    :param worksheet: str, worksheet name
    :return: worksheet object from openpyxl
    '''
    workbook = load_workbook(workbook)
    return workbook[worksheet]

# so now we have table definitions, let's get cacheDefinitions (instead of reading each pivotCacheDefinition.xml)
def data_extract(pivot_cache):
    '''
    Generic Function that returns both a DataFrame with the pivot table records and its dimensions in a dictionary
    :param pivot_cache: cacheDefinitions object from openpyxl
    :returns    DataFrame object: a pandas dataframe containing record's values and cacheFields names in columns
                dict_dim: dictionary containing all dimensions names and items from sharedItems fields
    '''
    record = []
    dims = {}
    dict_dim = {}
    for cf in df_pivot_cache.cacheFields:
        dims[cf.name] = cf.name
        dim_items = {}
        i = 0
        # let's get other items under this dimension
        for si in cf.sharedItems._fields:
            try:  # sometimes the list is empty
                dim_items[i] = si.v
                i += 1
            except:
                dim_items[i] = None
        dict_dim[cf.name] = dim_items

    # so now working with actual records
    for rows in df_pivot_cache.records.r:
        row = []
        for cols in rows._fields:
            try:  # we have to handle missing objects
                row.append(cols.v)
            except :
                row.append(None)
        record.append(row)
    # Let's turn it into a dataframe to work properly
    df = pd.DataFrame(columns=dims, data=record)

    remapped_columns = {}
    for c in df:
        def change_value(x):
            # Function that returns a given dictionary changing only values
            # that are not nan (avoids use of lambda)
            return dict_dim[c].get(x, x)
        c2 = df[c].apply(change_value)
        remapped_columns[c] = c2
    return df.assign(**remapped_columns)

def transform_data(table):
    transformed_df = pd.melt(table,
                            id_vars=['COMBUSTÍVEL', 'REGIÃO', 'ANO', 'ESTADO', 'UNIDADE', 'TOTAL'],
                            var_name='month',
                            value_name='volume') 
    transformed_df['month'] = transformed_df['month'].replace({
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
                                                    "ESTADO": "uf"
                                                    "ANO": "year_month",
                                                    "UNIDADE": "unit"
                                                    "COMBUSTÍVEL": "product"})
                                            
    transformed_df.drop(['TOTAL','REGIÃO'], axis=1, inplace=True)

    transformed_df = transformed_df.astype({"year_month":str})

    transformed_df['year_month'] = transformed_df['year_month'] + transformed_df['month']

    transformed_df['year_month'] = pd.to_datetime(transformed_df['year_month'], format=%Y%m)

    transformed_df.drop(['month'])

    transformed_df['created_at'] = pd.to_datetime('today')

    #Validate data if its correct

