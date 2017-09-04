<p align="left">
<img src="./img/postgrez.png" width="645px" height="250px" >
</p>

I extensively use the [giraffez](https://github.com/capitalone/giraffez) library in my daily work with Teradata. Outside of work, I mostly use PostgreSQL databases. I am consistently copying and pasting a database operations module (a set of psycopg2 wrapper functions) across projects, making various tweaks as I go (and directly violating [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself)...). As a result, I decided to create a package with similar functionality to giraffez, where the database connection is opened and automatically closed through Python's [with](http://effbot.org/zone/python-with-statement.htm) construct; a much better way for managing resources. The package has functionality for executing queries, uploading data from a Python object or a local flat file, and exporting data locally or into memory.

Emphasis on the **ez**.

## Installation
postgrez can be installed with pip:
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
```

## Usage

### Running Queries
The main parameter required to initiate all postgrez classes is the setup variable. The setup variable tells postgrez which database setup to use from the `~/.postgrez` config file (i.e. my_local_db, aws_db etc.).

```python
import postgrez

# Update table
with postgrez.Cmd('my_local_db') as c:
    query = 'update my_table set snap_dt=current_date'
    c.execute(query)

# Pass in variables to format your query
with postgrez.Cmd('my_local_db') as c:
    query = 'update my_table set snap_dt=%s where value=%s'
    c.execute(query, ('1900-01-01', 5))

# Select and retrieve resultset
with postgrez.Cmd('my_local_db') as c:
    query = 'select * from my_table limit 10'
    c.execute(query)
    resultset = c.cursor.fetchall()
    cols = [desc[0] for desc in c.cursor.description]

    # return the data as a list of dicts [{col1:val1, col2: val2, ..}, ..]
    results = [{cols[i]:value for i, value in enumerate(row)}
                        for row in resultset]
print (results)
```

### Loading Data
postgrez comes with two options for loading: loading from a Python list, or a local file. Both methods utilized the `psycopg2.connection.cursor.copy_from()` method, which is better practice than running a bunch of `INSERT INTO ` statements, see
[here](https://www.postgresql.org/docs/current/static/populate.html) and [here](https://www.depesz.com/2007/07/05/how-to-insert-data-to-database-as-fast-as-possible/).

```python

# load Python list into my_table
data = [(1, 2, 3), (4,5,6)]
with postgrez.Load('my_local_db') as l:
    l.load_to_object('my_table', data)


# load csv into my_table
with postgrez.Load('my_local_db') as l:
    l.load_to_file('my_table', 'my_file.csv')

# load other flat file into my_table
with postgrez.Load('my_local_db') as l:
    l.load_to_file('my_table', 'my_file.tsv', '|')

```

### Exporting Data
Exporting records from a table or query is accomplished with the `psycopg2.connection.cursor.copy_expert()` method, due to it's flexibility over the `copy_to()` method.

Similar to loading data, postgrez comes with two options for exporting. Records can be exported to a local file with the `Export.export_to_file()` method, or exported to a Python list with the `Export.export_to_object()` method.

```python
import postgrez

# export my_table to local file
with postgrez.Export('my_local_db') as e:
  e.export_to_file('my_table', 'results.csv')

# export the snap_dt column of my_table to local file
with postgrez.Export('my_local_db') as e:
  e.export_to_file('my_table', 'results.csv', columns=['snap_dt'])

# export a subset of my_table to local file
with postgrez.Export('my_local_db') as e:
  e.export_to_file("select * my_table where snap_dt='2017-01-01'", 'results.csv')

# export my_table to a Python variable
with postgrez.Export('my_local_db') as e:
  data = e.export_to_object("my_table")
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
postgrez.execute('my_local_db', query, ('1900-01-01', 5))
```

The most useful part of the execute wrapper is the results parsing and formatting, which eliminates the need for users to parse the column names from the cursor description.
```python
# Run query and return formatted resultset
data = postgrez.execute('my_local_db', 'select * from my_table limit 10')
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

data = postgrez.execute('my_local_db', query)
df = pd.DataFrame(data)
df.head()
```


#### Load Wrapper



#### Export Wrapper
The export wrapper uses `Export.export_to_file` if a filename is provided. Otherwise, `Export.export_to_object` is called and the associated records are returned.

```python
import postgrez

# export a subset of my_table to local file
postgrez.export("my_local_db", "select * my_table where snap_dt='2017-01-01'",
                  filename=results.csv)

# export the snap_dt column of my_table to local file
postgrez.export("my_local_db", "my_table", filename=results.csv,
                  columns=['snap_dt'])

# export my_table to a Python variable
data = postgrez.export("my_local_db", "my_table")
print (data)
```

## Resources
* [Docstring convention](http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)
* [giraffez](https://github.com/capitalone/giraffez)
*
