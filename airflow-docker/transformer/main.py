import pandas as pd
import numpy as np
from datetime import datetime

MONTH_DICT = {
    "Jan": 1, 
    "Fev": 2, 
    "Mar": 3, 
    "Abr": 4, 
    "Mai": 5,
    "Jun": 6, 
    "Jul": 7, 
    "Ago": 8, 
    "Set": 9, 
    "Out": 10, 
    "Nov": 11, 
    "Dez": 12
}

COLUMN_RENAME_DICT = {
    "ANO": "year",
    "COMBUSTÍVEL": "product",
    "ESTADO": "uf",
    "UNIDADE": "unit"
}

SCHEMA_DICT = {
    "year_month": "date",
    "uf": "string",
    "product": "string",
    "unit": "string",
    "volume": "double",
    "created_at": "timestamp"
}

def unpivot_table(df):
    # Unpivotting table by month
    df = pd.melt(df, id_vars = ["ANO", "ESTADO", "COMBUSTÍVEL", "UNIDADE"], value_vars = MONTH_DICT.keys(), var_name = "month", value_name = "volume")
    # Replacing month "names" by numbers
    return df.replace({"month": MONTH_DICT})

def create_year_month_column(df):
    # Converting "year" and "month" columns into "year_month"
    month_year_series = df["month"].astype(str).str.cat(df["year"].astype(str), sep = "/")
    df["year_month"] = pd.to_datetime(month_year_series, format = "%m/%Y")
    return df.drop(['month', 'year'], axis = 1)

def cast_table_columns(df):
    # Casting column types
    for column in SCHEMA_DICT.keys():
        if SCHEMA_DICT[column] == "string":
            df[column] = df[column].astype("string")
        elif SCHEMA_DICT[column] == "double":
            df[column] = df[column].astype(np.float64)
        elif SCHEMA_DICT[column] == "date":
            df[column] = pd.to_datetime(df[column], format = '%Y-%m')
        elif SCHEMA_DICT[column] == "timestamp":
            df[column] = pd.to_datetime(df[column])
    return df

def transform_data(source_path, target_path):
    df = pd.read_csv(source_path)
    df = unpivot_table(df)
    # Renaming columns
    df = df.rename(columns = COLUMN_RENAME_DICT)
    df = create_year_month_column(df)
    # Add "created_at" column
    df['created_at'] = datetime.now()
    # Ordering columns
    df = df[SCHEMA_DICT.keys()]
    df = cast_table_columns(df)
    #df.to_parquet(target_path, partition_cols = ["uf"])
    df.to_csv(target_path, index=False)
if __name__ == "__main__":
    transform_data("c:/airflow-docker/saida/diesel_uf_type.csv", "c:/airflow-docker/saida/diesel_uf_type")
    transform_data("c:/airflow-docker/saida/oil_uf_product.csv", "c:/airflow-docker/saida/oil_uf_product")