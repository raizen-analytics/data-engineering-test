Data Engineering Test
====================
## Schema

Data should be stored in the following format:

| Column       | Type        |
| ------------ | ----------- |
| `year_month` | `date`      |
| `uf`         | `string`    |
| `product`    | `string`    |
| `unit`       | `string`    |
| `volume`     | `double`    |
| `created_at` | `timestamp` |

## Information
1. Created an airflow container using docker_compose.

2. Created a directory on host with the dags directory './dags'

3. Created two python codes. One with all the main functions we'll be using. And other one containing our dag.

4. The dag consists of 3 tasks.
  >- extract_load - that loads data from pivot tables and transforms it into a usable csv format
  >- transform_oleos - that transforms data from our previously created csv file into the user requested table format and then into a parquet file for 'oleos'.All Data 
   is validates against original df in order to maintain file and data consistency
  >- transform_diesel - that transforms data from our previously created csv file into the user requested table format and then into a parquet file for 'diesel'.All 
   Data is validates against original df in order to maintain file and data consistency
  
## Things I could improve as time goes on

1. Further split my dag to optimize performance/usability
2. Further improve docker config
3. Work better with files in order to have every complete step in a different folder.
