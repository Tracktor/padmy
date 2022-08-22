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

You can quickly sample (ie: take a subset) of a database by simply running

```bash
poetry run cli sample \
  --db test --to-db test-sampled \
  --sample 20 \
  --schemas public
```

This will sample the `test` database into a new `test-sampled` database, copy of the
original one, keeping if possible **20%** of the original database.

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
