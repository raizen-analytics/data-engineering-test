from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd #Used to transform df to parquet in select folder
import shutil
from data.functions import *


default_args = {
    "owner": "Felipe Ramires",
    "start_date": days_ago(1)
}

@dag("ETL_Oleos_Combustiveis",
            default_args=default_args,
            schedule_interval="@monthly",
            tags=['raizen, felipe'],
            catchup=False) 

def etl_oleos_combustiveis():

    @task
    #extract data from excel pivot tables through its cached data and saves the 
    # organized tables as csvs for futher ETL operations
    #Creates one csv per needed pivot tables
    def extract_load():
        ws = load_worksheet(workbook="vendas-combustiveis.xlsx", worksheet="Plan1")
        try:
            pivot_oleo = ws._pivots[3].cache
            oleos = data_extract(pivot_oleo)
            oleos.to_csv("C:\Users\gamer\OneDrive\Documentos\raizen\extracted_data\oleos.csv", index=False)

            print('Dados extraídos - Oleos')

            pivot_diesel = ws._pivots[1].cache
            diesel = data_extract(pivot_diesel)
            diesel.to_csv("C:\Users\gamer\OneDrive\Documentos\raizen\extracted_data\diesel.csv", index=False)

            print('Dados extraídos - Diesel')

        except:
            print('Failed Load')

    extract_load()

    @task
    #TASK TO TRANSFORM 'OLEOS' DATA
    #Accesses the csv previously created to run all etl operations in the data
    #in order to achieve target table structure.
    #After transforming data, it validates if  data is consistent with the original pivot table
    #After validating the data, it its then saved as a parquet file as requested.
    def transform_oleos():
        raw_df = pd.read_csv("C:\Users\gamer\OneDrive\Documentos\raizen\extracted_data\oleos.csv")
        transform_data(raw_df)
        
        if (transformed_df['volume'].sum()/raw_df['TOTAL'].sum() >= 0.9999):
            print ('Dados conferidos e ok, pronto para processar')
            try:
                transformed_df.to_parquet("C:\Users\gamer\OneDrive\Documentos\raizen\final_data\oleos.gzip",compression = 'gzip')
                print('Parquet criado com sucesso em gzip')
            except:
                print('Erro ao criar Parquet')
        else: 
                print('Dados nao batem')

    transform_oleos()

    @task
    #TASK TO TRANSFORM 'DIESEL' DATA
    #Accesses the csv previously created to run all etl operations in the data
    #in order to achieve target table structure.
    #After transforming data, it validates if  data is consistent with the original pivot table
    #After validating the data, it its then saved as a parquet file as requested.
    def transform_diesel():
        raw_df = pd.read_csv("C:\Users\gamer\OneDrive\Documentos\raizen\extracted_data\diesel.csv")
        transform_data(raw_df)
        
        if (transformed_df['volume'].sum()/raw_df['TOTAL'].sum() >= 0.9999):
            try:
                transformed_df.to_parquet("C:\Users\gamer\OneDrive\Documentos\raizen\final_data\oleos.gzip",compression = 'gzip')
                print('Parquet criado com sucesso em gzip')
            except:
                print('Erro ao criar Parquet')
        else: 
            print('Dados nao batem')

    transform_diesel()

etl= etl_oleos_combustiveis()

