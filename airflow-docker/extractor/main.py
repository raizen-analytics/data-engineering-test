import xmltodict
import pandas as pd
import numpy as np
import sys
import os

# Variaveis do Projeto
assets_path = os.path.dirname(os.path.abspath(__file__))
datalake_path = os.path.dirname(os.path.abspath(__file__))

SOURCE_FILE_PATH = f"c:/airflow-docker/entrada/vendas-combustiveis-m3.xls"
SOURCE_DEFINITION_1 = f"c:/airflow-docker/entrada/xl/pivotCache/pivotCacheDefinition1.xml"
SOURCE_RECORDS_1 = f"c:/airflow-docker/entrada/xl/pivotCache/pivotCacheRecords1.xml"
SOURCE_DEFINITION_2 = f"c:/airflow-docker/entrada/xl/pivotCache/pivotCacheDefinition2.xml"
SOURCE_RECORDS_2 = f"c:/airflow-docker/entrada/xl/pivotCache/pivotCacheRecords2.xml"

EXCEL_ROW_HEADER_1 = 52
EXCEL_COLUMNS_1 = "B:W"
EXCEL_ROW_HEADER_2 = 132
EXCEL_COLUMNS_2 = "B:J"

COMPARASION_PRECISION = 0.000001

FIELDS_ORDER_1 = ["COMBUSTÍVEL", "ANO", "ESTADO"]
FIELDS_ORDER_2 = ["ANO", "COMBUSTÍVEL", "ESTADO"]

MONTH_DICT = {
    0: "Jan",
    1: "Fev",
    2: "Mar",
    3: "Abr",
    4: "Mai",
    5: "Jun",
    6: "Jul",
    7: "Ago",
    8: "Set",
    9: "Out",
    10: "Nov",
    11: "Dez",
    12: "Total"
}


# Functions de tratamento do arquivo .xls
def get_dict_from_xml_file(file):
    with open(file, "r", encoding="utf8") as f:
        return_dict = xmltodict.parse(f.read())
    return return_dict

def get_definition_fields(definition):
    doc_def = get_dict_from_xml_file(definition)
    return doc_def['pivotCacheDefinition']['cacheFields']['cacheField']

def get_definition_field_name_values(field):
    name = None
    items = None
    if field.get('sharedItems') is not None:
        for attrib in ['s', 'n']:
            if field['sharedItems'].get(attrib) is not None:
                name = field['@name']
                items = [item['@v'] for item in field['sharedItems'][attrib]]
    return name, items

def get_definition_filters(fields):
    filters = {}
    for field in fields:
        field_name, field_values = get_definition_field_name_values(field)
        if field_name is not None:
            filters[field_name] = field_values
    return filters

def get_definition_df(definition, fields_order):
    fields = get_definition_fields(definition)
    filters = get_definition_filters(fields)

    df = None
    for filter_field in fields_order:
        df_temp = pd.DataFrame(filters[filter_field], columns = [filter_field])
        if df is not None:
            df = df.merge(df_temp, how = "cross")
        else:
            df = df_temp
    return df

def get_values_df(records):
    doc_rec = get_dict_from_xml_file(records)
    data_months = []
    for record in doc_rec['pivotCacheRecords']['r']:
        values = [value['@v'] for value in record['n']]
        data_month = {}
        data_month["UNIDADE"] = record['s']['@v']
        for index in range(0, len(values)):
            data_month[MONTH_DICT[index]] = values[index] 
        data_months.append(data_month)
    return pd.DataFrame(data_months)

def fix_value_placement(df):
    index_list = df[df["ANO"] == "2020"].index.to_list()
    for index in index_list:
        df["Total"].iloc[index] = df["Out"].iloc[index] 
        df["Out"].iloc[index] = np.nan
    return df

def convert_to_pivot_table(df):
    df = fix_value_placement(df)
    df = df.drop(["COMBUSTÍVEL", "ESTADO", "UNIDADE"],axis = 1)
    df = df.rename(columns = {"ANO": "Dados"})
    for column in df.columns:
        if column != "Dados":
            df[column] = df[column].astype(float)
    df = df.set_index("Dados")
    df = df.groupby("Dados").sum().T
    df.index = df.index.str.slice(start = 0, stop = 3)
    return df

def extract_raw_pivot_table_xls(cols, header):
    df = pd.read_excel(SOURCE_FILE_PATH, usecols = cols, header = header).iloc[:13,]
    for column in df.columns:
        if column != "Dados":
            df[column] = df[column].astype(float)
    df["Dados"] = df["Dados"].str.slice(start = 0, stop = 3)
    df = df.fillna(0)
    return df.set_index("Dados")

def check_df_values(df, excel_cols, excel_header):
    df_pivot = convert_to_pivot_table(df)
    df_excel = extract_raw_pivot_table_xls(excel_cols, excel_header)
    df_excel.columns = df_pivot.columns
    return (df_pivot - df_excel).lt(COMPARASION_PRECISION).all().all()

def extract_oil_uf_product_df():
    df_filters = get_definition_df(SOURCE_DEFINITION_1, FIELDS_ORDER_1)
    df_month = get_values_df(SOURCE_RECORDS_1)
    df =  pd.concat([df_filters, df_month], axis = 1)
    if check_df_values(df, EXCEL_COLUMNS_1, EXCEL_ROW_HEADER_1):
        return df
    else:
        raise Exception("Dataframe values are different from source data")

def extract_diesel_uf_type_df():
    df_filters = get_definition_df(SOURCE_DEFINITION_2, FIELDS_ORDER_2)
    df_month = get_values_df(SOURCE_RECORDS_2)
    df =  pd.concat([df_filters, df_month], axis = 1)
    if check_df_values(df, EXCEL_COLUMNS_2, EXCEL_ROW_HEADER_2):
        return df
    else:
        raise Exception("Dataframe values are different from source data")

if __name__=="__main__":
    
   #extrai os dados e salva em csv
   extract_diesel_uf_type_df().to_csv('c:/airflow-docker/saida/diesel_uf_type.csv', index=False)
   extract_oil_uf_product_df().to_csv('c:/airflow-docker/saida/oil_uf_product.csv', index=False)
   