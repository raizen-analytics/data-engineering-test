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

## selecting column range

data = data.select('_c1','_c2','_c3','_c4','_c5','_c6','_c7','_c8','_c9','_c10','_c11','_c12','_c13','_c14','_c15','_c16','_c17','_c18','_c19','_c20','_c21','_c22', '_c23')

## renaming the columns

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

## creating a temp view

data.createOrReplaceTempView("tmp_data")

## loading dataframe data with limit and having only what matters in the dataframe

data = spark.sql("select * from tmp_data limit 188")

## creating table sales_petroleum_derived_fuels (Sales of oil derivative fuels by UF and product)

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

## grouping and unpivot logic

from pyspark.sql.functions import expr, col, sum

### table name definition
base_sales = "sales_petroleum_derived_fuels"

### list of columns that should not be unpivoted
non_melt_columns = ["uf", "product"]

### unpivot on year columns in rows
df_melted = data.selectExpr(*non_melt_columns, 
                          "stack(22, " + ", ".join([f"'{col_name}', {col_name}" for col_name in data.columns if col_name not in non_melt_columns]) + ") as (year_month, total_volume)")

### adding the year_month column
df_with_year_month = df_melted.withColumn("year_month", col("year_month").cast("date"))

### grouping by uf, product, unit, year_month and calculating the sum of the volume column
df_aggregated = df_with_year_month.groupBy("uf", "product", "unit", "year_month").agg(sum("total_volume").alias("volume"))

### selecting the desired columns
data = df_aggregated.select("year_month", "uf", "product", "unit", "volume", "created_at")

### saving Dataframe to table in Databricks
data.write.mode("overwrite").saveAsTable(base_sales)

## describe column

%sql
desc sales_petroleum_derived_fuels;
![image](https://github.com/msap89/data-engineering-test/assets/152655536/67e11d92-5b17-4ecf-9232-a916a9882075)

## Table creation sales_diesel (Sales of diesel by UF and type)

### Table creation sales_petroleum_derived_fuels

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

### condition to bring only Diesel
diesel = data.where(col("product") == "ÓLEO DIESEL (m3)")

### saving the diesel dataframe with table name sales_diesel
data.write.mode("overwrite").saveAsTable('sales_diesel')

