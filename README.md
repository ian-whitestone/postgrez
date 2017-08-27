<p align="left">
<img src="./img/postgrez.png" width="645px" height="250px" >
</p>

I extensively use the [giraffez](https://github.com/capitalone/giraffez) library in my daily work with Teradata. Outside of work, I mostly use PostgreSQL databases. I am consistently copying and pasting a database operations module (a set of psycopg2 wrapper functions) across projects, making various tweaks as I go (and directly violating [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself)...). As a result, I decided to create a package with similar functionality to giraffez, where the database connection is opened and automatically closed through Python's [with](http://effbot.org/zone/python-with-statement.htm) construct; a much better way for managing resources.

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
  database: nba
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

If you don't want to be embedding the `with ...` code throughout your modules, I have provided some wrapper functions to further simplify:

```python
import postgrez

data = postgrez.query('my_local_db', 'select * from my_table limit 10')
print (data)

query = """
CREATE TEMPORARY TABLE my_temp_table AS (
  SELECT *
  FROM my_table
  WHERE x=5
);

SELECT * FROM my_temp_table;
"""

import pandas as pd

data = postgrez.query('my_local_db', 'select * from my_table limit 10')
df = pd.DataFrame(data)
df.head()
```

### Resources
* [Docstring convention](http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)
* [giraffez](https://github.com/capitalone/giraffez)
*
