# Padmy

CLI utility functions for Postgresql such as **sampling** and **anonymization**.

## Installation

Run `poetry install`  to install the python packages.

## 1. Database Exploration

You can get information about a database by running

```bash
poetry run cli analyze --db test --schemas test
```

or using the docker image

```bash
 docker run -it \
   --network host \
   tracktor/padmy:latest analyze --db test --schemas test
```

For instance, the following table definition will output:

```sql
CREATE TABLE table1
(
    id SERIAL PRIMARY KEY
);

CREATE TABLE table2
(
    id        SERIAL PRIMARY KEY,
    table1_id INT REFERENCES table1
);

CREATE TABLE table3
(
    id        SERIAL PRIMARY KEY,
    table1_id INT REFERENCES table1,
    table2_id INT REFERENCES table2
);

CREATE TABLE table4
(
    id        SERIAL PRIMARY KEY,
    table1_id INT REFERENCES table1
);
INSERT INTO table1(id)
SELECT generate_series(0, 10);
```

**Default**

![Network schema](./docs/explore-default.png)

**Network Schema** (if `--show-graphs` is specified)

![Network schema](./docs/explore-schema.png)

## 2. Sampling

You can quickly sample (ie: take a subset) of a database by running

```bash
poetry run cli sample \
  --db test --to-db test-sampled \
  --sample 20 \
  --schemas public
```

This will sample the `test` database into a new `test-sampled` database, copy of the
original one, keeping if possible (see: [Annexe](#Known-limitations)) **20%** of the original database.

You can choose how to sample with more granularity by passing a configuration file.
Here is an example:

```yaml
# We want a default sampling size of 20% of each table count
sample: 20
# We want to sample `schema_1` and `schema_2`
schemas:
  - schema_1
  # We want a default size of 30% for the tables of this schema
  - name: schema_2
    sample: 30

tables:
  # We want a sample size of 10% for this table
  - schema: public
    table: table_3
    sample: 10
```

## 3. Migration utils

**Setting up**

This library includes a migration utility to help you evolve your data model.
In order to use it, start by setting up the migration table:

```bash
poetry run cli -vv migrate setup --db postgres
```

This will create the `public.migration` table that stores all the migration / rollback that
will be applied.

**Setting up the Schemas**

Now that we are all setup, let's create our first sql file that will create the schema:

```bash
poetry run cli -vv migrate new-sql 1 --sql-dir /tmp/sql
```  

Add `CREATE SCHEMA general;` to the file.

Then apply the modifications to the database:

```bash
poetry run cli -vv migrate apply-sql --sql-dir /tmp/sql --db postgres 
```

Notes:
This will run through all the files in the `/tmp/sql` folder (in order) run them.
Sql files here **need to be IDEMPOTENT**

**Creating a first migration**

Now, lets create our first migration:

```bash
mkdir -p /tmp/migrations # You can choose a different folder to store your migrations
poetry run cli -vv migrate new --sql-dir /tmp/migrations
```

This will create 2 new files:

- **up**: `{timestamp}-{migration_id}-up.sql` that contains your
  migration to apply to the database.
- **down**: `{timestamp}-{migration_id}-down.sql` that contains the code to revert your changes.

Let's now modify the `up.sql` file with:

```sql
CREATE TABLE IF NOT EXISTS general.test
(
    id  int primary key,
    foo int
);

CREATE TABLE IF NOT EXISTS general.test2
(
    id  serial primary key,
    foo text
);
```

and check that the migration is valid:

```bash
poetry run cli -vv migrate verify --sql-dir /tmp/migrations
``` 

Because we did not add anything to the `down.sql` file, the command returns an error.
Let's modify it to make the command pass:

```sql
DROP table general.test;
DROP table general.test2;
``` 

```bash
poetry run cli -vv migrate verify --sql-dir /tmp/migrations
``` 

We are all good !

## 4. Comparing databases schemas

You can compare two databases by running:

```bash
poetry run cli -vv schema-diff --db tracktor --schemas schema_1,schema_2
```
If differences are found, the command will output the differences between the two databases.


### Known limitations

**Exact sample size**

Sometimes, we cannot guaranty that the sampled table will have the exact
expected size.

For instance let's say we want **10%** of *table1* and **10%** of *table2*, given the following
table definitions:

```sql
CREATE TABLE table1
(
    id SERIAL PRIMARY KEY
);

CREATE TABLE table2
(
    id        SERIAL PRIMARY KEY,
    table1_id INT NOT NULL REFERENCES table1
);

INSERT INTO table1(id)
VALUES (1);

INSERT INTO table2(table1_id)
SELECT 1
FROM generate_series(1, 10);
```

In this case, it's not possible to have less that **100%** of table 1 since it has only 1 key on
which depend all the `table1_id` rows of *table2*.

**Cyclic foreign keys**

Cyclic foreign keys (table with a FK on another table that reference the previous one) are not supported.
Here is an example.

```sql
CREATE TABLE table1
(
    id        SERIAL PRIMARY KEY,
    table2_id INT NOT NULL
);

CREATE TABLE table2
(
    id        SERIAL PRIMARY KEY,
    table1_id INT NOT NULL
);

ALTER TABLE table1
    ADD CONSTRAINT table1_table2_id_fk
        FOREIGN KEY (table2_id) REFERENCES table2;

ALTER TABLE table2
    ADD CONSTRAINT table2_table1_id_fk
        FOREIGN KEY (table1_id) REFERENCES table1;
```

![Cyclic dependencies](./docs/cyclic-deps.png)

You can display cycling dependencies in a database by running:

```bash
poetry run cli -vv analyze --db test --schemas test --show-graph
```

(**Note::** you'll need to have installed the `network` extra )

**Self referencing foreign keys**

Foreign keys referencing another column in the same table are ignored.

```sql
CREATE TABLE table1
(
    id        SERIAL PRIMARY KEY,
    parent_id INT REFERENCES table1
);
```

# Annexes

## Showing Network in Jupyter

You can display the network visualization in Jupyter using [jupyter_dash]()

```python
from jupyter_dash import JupyterDash
from padmy.sampling import network, viz, sampling
import asyncpg

PG_URL = 'postgresql://postgres:postgres@localhost:5432/test'

app = JupyterDash(__name__)

db = sampling.Database(name='test')

async with asyncpg.create_pool(PG_URL) as pool:
    await db.explore(pool, ['public'])

g = network.convert_db(db)

app.layout = viz.get_layout(g,
                            style={'width': '100%', 'height': '800px'},
                            layout='klay')

app.run_server(mode='jupyterlab')  # or mode='inline'
```
