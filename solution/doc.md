# Solution To-Be

## Phase 1

- Automate data extraction (wrapper to site)
- Transform data (reach the desired goal in README)
- Load data using sqlalchemy (to allow loading to any SQL database at least)

## Phase 2

- Orchestrate this data flow (create a pipeline)
- Containerizate the orchestration
- Consolidate data

# Solution As-Is

- importConfigs

There is a schema defined with configurations for the wrapper and database. This
file ('config.json') must be placed on the root of the execution. The file must
contain a valid url for the wrapper and keywrods to be looked-up inside the
data extracted. For the database, must cointain the name for itself and a
default schema with columns names and types - for SQL instances.

## Extraction

- downloadSheet

One simple definition using the requests lib to get the url and write to a file.

- getPivotSourceData

A definition that uses xlwings lib to open the excel sheets as well as a macro
sheet. Once they are opened (currently in quiet/invisible mode), two macros run,
one for each sheet (oil and diesel). They have a fixed range inside them that
enables a selection in each cell specified within the range. On each one, it's
simulated a "Show More Details" or "cell.ShowDetail = True", which expands the
source data of the pivot table into new sheets. Once all the source data is
expanded, the root sheet or "Plan1" is deleted and the file is saved and closed.

## Database

For the purpose of using a SQL and light-weight database version integrated with
Python, the best option is sqlite3.

- createDB

Using the configs imported, starts a new sqlite3 engine and starts a connection.

- createTables

Currently not receiving parameters as I'm using the configs of the file, creates
tables IF NOT EXISTS using the defaultSchema specified and the names of wrapper/keywords.

- select / truncate

Has a table as input and perform those operations (on sqlite3 TRUNCATE is DELETE)

- insert

Has a table, fields (strings) and values (tuple) as input. To enable a good
performance, I decided to use 'executemany' as default for inserting new data.
It avoids creating a new cursor for every inserted row and works like a BULK.
The values MUST be sent as tuple and not specified on the code for security
measures (instead I'm using ? marks and sending the tuples to the cursor).

## Transformation

The finest part. I first declare a dict with the months to be read on each row
and the respective translation to the equivalent in english (needed for cast
types string to datetime).

I have spent long hours here, you can see by the times I have changed this
modules on the git log/track. I came from 10 minutes execution to 1 minute, I
guess this is my best (yet).

I use pandas and datetime on this object.

- parseSheet

Has a dataframe as input and a sheet (entire file). I drop columns without full
missing rows i.e. last months of 2020. Then I start iterating on each for each
sheet.

- buildDocs

Has a row as input. Collects all months within the row and starts iterate over
them. I create a list to append all months and a dict for each month
(performance reasons). For each month I assemble the needed fields and append
them AS A LIST OF VALUES to the bigger list. Once all months are iterated,
I cast the big list as tuple and load them to the database.

## Comments

Thats some points to improve performance, security and error-checking. But its
ready to be managed by orchestration and containerization tools.

KISS and hack the planet.
