# Databricks notebook source
## base location and type
file_location = "/FileStore/tables/raw/vendas_combustiveis_m3.csv"
file_type = "csv"

## read base file
data = spark.read.format(file_type) \
  .option("inferSchema", "false") \
  .option("header", "false") \
  .option("header", "false") \
  .option("sheet_name", "Plan1") \
  .option("sep", ";") \
  .load(file_location, skiprows = 54)

## selecionando range de colunas

data = data.select('_c1','_c2','_c3','_c4','_c5','_c6','_c7','_c8','_c9','_c10','_c11','_c12','_c13','_c14','_c15','_c16','_c17','_c18','_c19','_c20','_c21','_c22', '_c23')

## renomeando as colunas

data = (data.withColumnRenamed(data.columns[0], "uf")
			.withColumnRenamed(data.columns[1],"product")
			.withColumnRenamed(data.columns[2], "2000")
			.withColumnRenamed(data.columns[3], "2001")
			.withColumnRenamed(data.columns[4], "2002")
			.withColumnRenamed(data.columns[5], "2003")
			.withColumnRenamed(data.columns[6], "2004")
			.withColumnRenamed(data.columns[7], "2005")
			.withColumnRenamed(data.columns[8], "2006")
			.withColumnRenamed(data.columns[9], "2007")
			.withColumnRenamed(data.columns[10], "2008")
			.withColumnRenamed(data.columns[11], "2009")
			.withColumnRenamed(data.columns[12], "2010")
			.withColumnRenamed(data.columns[13], "2011")
			.withColumnRenamed(data.columns[14], "2012")
			.withColumnRenamed(data.columns[15], "2013")
			.withColumnRenamed(data.columns[16], "2014")
			.withColumnRenamed(data.columns[17], "2015")
			.withColumnRenamed(data.columns[18], "2016")
			.withColumnRenamed(data.columns[19], "2017")
			.withColumnRenamed(data.columns[20], "2018")
			.withColumnRenamed(data.columns[21], "2019")
			.withColumnRenamed(data.columns[22], "2020"))

## criando uma temp view

data.createOrReplaceTempView("tmp_data")

## carregando dataframe data com limit e ter somente o que importa no dataframe

data = spark.sql("select * from tmp_data limit 188")

## criando tabela sales_petroleum_derived_fuels (Sales of oil derivative fuels by UF and product)

spark.sql('''
  CREATE TABLE IF NOT EXISTS sales_petroleum_derived_fuels (
    year_month DATE,
    uf STRING,
    product STRING,
    unit STRING,
    volume DOUBLE,
    created_at TIMESTAMP
  )
''')

## logica do agrupamento e unpivot

from pyspark.sql.functions import expr, col, sum

### definicao table name
base_sales = "sales_petroleum_derived_fuels"

### lista de colunas que não devem ser despivotada
non_melt_columns = ["uf", "product"]

### despivot nas colunas de ano em linhas
df_melted = data.selectExpr(*non_melt_columns, 
                          "stack(22, " + ", ".join([f"'{col_name}', {col_name}" for col_name in data.columns if col_name not in non_melt_columns]) + ") as (year_month, total_volume)")

### adicionando a coluna year_month
df_with_year_month = df_melted.withColumn("year_month", col("year_month").cast("date"))

### agrupando por uf, product, unit, year_month e calculando a soma da coluna volume
df_aggregated = df_with_year_month.groupBy("uf", "product", "unit", "year_month").agg(sum("total_volume").alias("volume"))

### selecionando as colunas desejadas
data = df_aggregated.select("year_month", "uf", "product", "unit", "volume", "created_at")

### salvando o DataFrame na tabela no Databricks
data.write.mode("overwrite").saveAsTable(base_sales)

## describe column

%sql
desc sales_petroleum_derived_fuels;
![image](https://github.com/msap89/data-engineering-test/assets/152655536/67e11d92-5b17-4ecf-9232-a916a9882075)

## Criacao da table sales_diesel 

### criando tabela sales_petroleum_derived_fuels ----------

spark.sql('''
  CREATE TABLE IF NOT EXISTS sales_diesel (
    year_month DATE,
    uf STRING,
    product STRING,
    unit STRING,
    volume DOUBLE,
    created_at TIMESTAMP
  )
''')

### condicao para trazer somente Diesel
diesel = data.where(col("product") == "ÓLEO DIESEL (m3)")

### salvando o dataframe diesel com table name sales_diesel
data.write.mode("overwrite").saveAsTable('sales_diesel')

