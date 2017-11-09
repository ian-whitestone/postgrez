<p align="left">
<img src="./img/postgrez.png" width="645px" height="250px" >
</p>

I extensively use the [giraffez](https://github.com/capitalone/giraffez) library in my daily work with Teradata. Outside of work, I mostly use PostgreSQL databases. I am consistently copying and pasting a database operations module (a set of psycopg2 wrapper functions) across projects, making various tweaks as I go (and directly violating [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself)...). As a result, I decided to create a package with similar functionality to giraffez, where the database connection is opened and automatically closed through Python's [with](http://effbot.org/zone/python-with-statement.htm) construct; a much better way for managing resources. Much of the framework for postgrez was based off another Capital One internal package called redfox (not open-source, yet), created by two pretty awesome data scientists, [Ian](https://github.com/theianrobertson) and [Faisal](https://github.com/fdosani), so shoutout to them. 

The package has functionality for executing queries, uploading data from a Python object or a local flat file, and exporting data locally or into memory. Emphasis on the **ez**.

## Installation
postgrez can be installed with pip & setuptools:

First, make sure you upgrade (or install) setuptools:
```
$ pip install --upgrade setuptools
```

Then install:
```
$ pip install git+https://github.com/ian-whitestone/postgrez.git
```

You must also manually add a YAML configuration file named `.postgrez` in your home directory. The config file will contain the required database connection information. You can keep all your database connection info in this file.

postgrez expects the YAML config file, `~/.postgrez` in the following format:

```yml
my_local_db:
  host: localhost
  user: my_user_name
  port: 5432
  database: my_local_db_name
```

At the minimum, you must supply a host, user and database. If no port is provided, the default port 5432 will be used.

You can add additional database setups in your config:

```yml
my_local_db:
  host: localhost
  user: my_user_name
  port: 5432
  database: my_local_db_name

aws_db:
    host: host_name.rds.amazonaws.com
    user: my_user_name
    password: my_passwd
    database: aws_db_name
    port: 5432

default: my_local_db # Optionally add this parameter to specify the default setup to be used
```


## Usage

### Running Queries
The main parameter required to initiate all postgrez classes is the setup variable. The setup variable tells postgrez which database setup to use from the `~/.postgrez` config file (i.e. my_local_db, aws_db etc.).

```python
import postgrez

# Update table
with postgrez.Cmd(setup='my_local_db') as c:
    query = 'update my_table set snap_dt=current_date'
    c.execute(query=query)

# Pass in variables to format your query
with postgrez.Cmd(setup='my_local_db') as c:
    query = 'update my_table set snap_dt=%s where value=%s'
    c.execute(query=query, query_vars=('1900-01-01', 5))

# Select and retrieve resultset
with postgrez.Cmd(setup='my_local_db') as c:
    query = 'select * from my_table limit 10'
    c.execute(query=query)
    resultset = c.cursor.fetchall()
    cols = [desc[0] for desc in c.cursor.description]

    # return the data as a list of dicts [{col1:val1, col2: val2, ..}, ..]
    results = [{cols[i]:value for i, value in enumerate(row)}
                        for row in resultset]
print (results)

# If a default setup was specified in ~/.postgrez, the setup variable can be omitted
with postgrez.Cmd() as c:
    query = 'update my_table set snap_dt=current_date'
    c.execute(query=query)
```

### Loading Data
postgrez comes with two options for loading: loading from a Python list, or a local file. Both methods utilize the `psycopg2.connection.cursor.copy_from()` method, which is better practice than running a bunch of `INSERT INTO ` statements, see
[here](https://www.postgresql.org/docs/current/static/populate.html) and [here](https://www.depesz.com/2007/07/05/how-to-insert-data-to-database-as-fast-as-possible/).

```python

# load Python list into my_table
data = [(1, 2, 3), (4, 5, 6)]
with postgrez.Load() as l:
    l.load_from_object(table_name='my_table', data=data)

# load csv into my_table
with postgrez.Load() as l:
    l.load_from_file(table_name='my_table', filename='my_file.csv')

# load other flat file into my_table
with postgrez.Load() as l:
    l.load_from_file(table_name='my_table', filename='my_file.tsv',
                      delimiter='|')

```

In the examples shown above, the columns in the files and data object are expected to be in the same order as the columns in `my_table`. If this is not the case, the columns parameter must be supplied.

```python

# load Python list into my_table
data = [(3, 2, 1), (6, 5, 4)]
with postgrez.Load('my_local_db') as l:
    l.load_to_object(table_name='my_table', data=data,
                      columns=['col3','col2','col1'])

data = [(2, 3, 1), (5, 6, 4)]
with postgrez.Load('my_local_db') as l:
    l.load_to_object(table_name='my_table', data=data,
                      columns=['col2','col3','col1'])

```

### Exporting Data
Exporting records from a table or query is accomplished with the `psycopg2.connection.cursor.copy_expert()` method, due to it's flexibility over the `copy_to()` method.

Similar to loading data, postgrez comes with two options for exporting. Records can be exported to a local file with the `Export.export_to_file()` method, or exported to a Python list with the `Export.export_to_object()` method.

```python
import postgrez

# export my_table to local file
with postgrez.Export() as e:
  e.export_to_file(query='my_table', filename='results.csv')

# export the snap_dt column of my_table to local file
with postgrez.Export() as e:
  e.export_to_file(query='my_table', filename='results.csv',
                    columns=['snap_dt'])

# export a subset of my_table to local file
with postgrez.Export() as e:
  e.export_to_file(query="select * my_table where snap_dt='2017-01-01'",
                    filename='results.csv')

# export my_table to a Python variable
with postgrez.Export() as e:
  data = e.export_to_object(query="my_table")
print (data)
```

Note: Exporting data into Python using the `Export.export_to_object()` method provides no performance increase over running a `select * from my_table` with the `Cmd.execute()` method.


### Wrapper Functions

If you don't want to be embedding the `with ...` code throughout your modules, I have provided some wrapper functions to further simplify.

#### Cmd Wrapper
Similar to above, the execute function can be run with a query and set of variables.

```python
import postgrez

# Run execute with variables
query = 'update my_table set snap_dt=%s where value=%s'
postgrez.execute(query=query, query_vars=('1900-01-01', 5), setup='my_local_db')
```

The most useful part of the execute wrapper is the results parsing and formatting, which eliminates the need for users to parse the column names from the cursor description.
```python
# Run query and return formatted resultset, using default setup
data = postgrez.execute(query='select * from my_table limit 10')
print (data)

# Create a temporary table, read results from query into pandas dataframe
import pandas as pd

query = """
CREATE TEMPORARY TABLE my_temp_table AS (
  SELECT *
  FROM my_table
  WHERE x=5
);

SELECT * FROM my_temp_table;
"""

data = postgrez.execute(query=query)
df = pd.DataFrame(data)
df.head()
```


#### Load Wrapper
The load wrapper uses `Load.load_from_file` if a filename is provided. Alternatively, if the `data` arg is provided, `Load.load_from_object` is called.

```python
import postgrez

# load Python list into my_table
data = [(1, 2, 3), (4, 5, 6)]
postgrez.load(table_name='my_table', data=data, setup='my_local_db')

# load csv into my_table
postgrez.load(table_name='my_table', filename='my_file.csv')
```

#### Export Wrapper
The export wrapper uses `Export.export_to_file` if a filename is provided. Otherwise, `Export.export_to_object` is called and the associated records are returned.

```python
import postgrez

# export a subset of my_table to local file
postgrez.export(query="select * my_table where snap_dt='2017-01-01'",
                  filename=results.csv, setup='my_local_db')

# export the snap_dt column of my_table to local file
postgrez.export(query="my_table", filename=results.csv,
                  columns=['snap_dt'])

# export my_table to a Python variable
data = postgrez.export(query="my_table")
print (data)
```
