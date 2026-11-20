# PySpark Cluster Pilot

A local Apache Spark **standalone cluster** running in Docker, with **JupyterLab** as the driver. It's a learning project for three things:

1. **Cluster configuration**: how a Spark standalone cluster fits together (master, workers, executors, cores, memory, history server).
2. **PySpark practice**: DataFrames, schemas, transformations, aggregations, window functions, joins, partitioning, UDFs, broadcast variables, and accumulators.
3. **Spark UI observability**: reading jobs, stages, tasks, DAGs, shuffles, and physical plans in the Spark UI and History Server.

## Architecture

```
                 ┌──────────────────────────┐
                 │  jupyter (driver)        │  :8888 JupyterLab
                 │  PySpark 4.1.2           │  :4040 Spark application UI
                 └────────────┬─────────────┘
                              │ spark://spark-master:7077
                 ┌────────────▼─────────────┐
                 │  spark-master            │  :8080 Master UI
                 └──────┬────────────┬──────┘
          ┌─────────────▼───┐   ┌────▼────────────┐
          │ spark-worker-1  │   │ spark-worker-2  │
          │ 4 cores · 4 GB  │   │ 4 cores · 4 GB  │
          │ UI :8081        │   │ UI :8082        │
          └─────────────────┘   └─────────────────┘

                 ┌──────────────────────────┐
                 │  spark-history           │  :18080 History Server
                 └──────────────────────────┘
```

All services use the official `spark:4.1.2-java21-python3` image and share the `spark-cluster-net` Docker network. The Jupyter image ([Dockerfile.jupyter](Dockerfile.jupyter)) adds JupyterLab and `pyspark==4.1.2` on top of the same image, so the driver and the executors run matching Spark and Python versions.

### Shared volumes

| Host path      | Container path         | Purpose                                   |
|----------------|------------------------|-------------------------------------------|
| `./data`       | `/home/spark/data`     | Input and output datasets (shared by all nodes) |
| `./conf`       | `/opt/spark/conf`      | Spark configuration (e.g. `spark-defaults.conf`) |
| `./events`     | `/tmp/spark-events`    | Event logs read by the History Server     |
| `./notebooks`  | `/home/spark/notebooks`| Notebooks (Jupyter only)                  |

Every node mounts `./data` at the same path, so executors on any worker can read and write the files the driver refers to.

## Getting started

### Prerequisites

- Docker with Docker Compose v2
- About 8 GB of RAM free for Docker (each worker offers 4 GB)

### Start the cluster

```bash
make setup_spark-cluster
```

Then get the JupyterLab token from the logs:

```bash
make get_jupyter_logs
```

### Web UIs

| UI                     | URL                     |
|------------------------|-------------------------|
| JupyterLab             | http://localhost:8888   |
| Spark Master           | http://localhost:8080   |
| Worker 1               | http://localhost:8081   |
| Worker 2               | http://localhost:8082   |
| Application UI (live)  | http://localhost:4040   |
| History Server         | http://localhost:18080  |

The application UI on port 4040 is only up while a `SparkSession` is running in a notebook.

### Enable the History Server

The History Server only shows applications that wrote event logs. Create `conf/spark-defaults.conf` with:

```properties
spark.eventLog.enabled           true
spark.eventLog.dir               file:///tmp/spark-events
spark.history.fs.logDirectory    file:///tmp/spark-events
```

Restart the cluster afterwards. Finished applications will then show up at http://localhost:18080.

### Stop the cluster

```bash
make down_spark-cluster   # stop containers and remove volumes
make nuke_spark-cluster   # same, and also delete ./data and ./events
```

> ⚠️ `nuke_spark-cluster` deletes your local datasets.

## Connecting to the cluster from a notebook

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName('pyspark-pilot')
    .master('spark://spark-master:7077')
    .config('spark.executor.instances', 1)  # Ignored by the standalone cluster manager
    .config('spark.cores.max', '4')         # Workaround: caps total cores → 2 executors of 2 cores
    .config('spark.executor.cores', 2)
    .config('spark.executor.memory', '512M')
    .getOrCreate()
)
```

To make shuffles, joins, and partitions easier to read in the Spark UI, the later notebooks turn off Adaptive Query Execution and automatic broadcast joins:

```python
spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

Use `.master('local[*]')` to run everything inside the Jupyter container without the cluster.

## Notebooks

Notebooks 1–9 run in `local[*]` mode and cover the PySpark API. From notebook 10 on, they connect to the standalone cluster and focus on execution and the Spark UI.

| Notebook | Mode | Topics |
|----------|------|--------|
| [notebook1](notebooks/notebook1.ipynb)   | local   | SparkSession, creating DataFrames, partitions, actions vs. transformations, writing CSV |
| [notebook2](notebooks/notebook2.ipynb)   | local   | Schemas (DDL strings vs. `StructType`), `col`/`expr`, `select`, `selectExpr`, casting |
| [notebook3](notebooks/notebook3.ipynb)   | local   | Reading CSV with a schema, `withColumn`, `lit`, rename, drop, `withColumns` |
| [notebook4](notebooks/notebook4.ipynb)   | local   | `when`/`otherwise`, date parsing and formatting, current date/timestamp |
| [notebook5](notebooks/notebook5.ipynb)   | local   | `union`, ordering, `groupBy`/`agg`, filtering after aggregation ("having") |
| [notebook6](notebooks/notebook6.ipynb)   | local   | `distinct`, window functions, `row_number`, window functions in SQL expressions |
| [notebook7](notebooks/notebook7.ipynb)   | local   | `spark_partition_id`, `repartition` by column, joins |
| [notebook8](notebooks/notebook8.ipynb)   | local   | Reading CSV: explicit schema vs. `inferSchema` |
| [notebook9](notebooks/notebook9.ipynb)   | local   | Parquet I/O, `partitionBy` on write, partition discovery on read |
| [notebook10](notebooks/notebook10.ipynb) | cluster | First job on the cluster: executor config, Parquet read/write |
| [notebook11](notebooks/notebook11.ipynb) | cluster | Python UDFs |
| [notebook12](notebooks/notebook12.ipynb) | local   | AQE off, `defaultParallelism`, repartition + join, reading `explain()` plans and shuffle stages |
| [notebook13](notebooks/notebook13.ipynb) | cluster | Aggregation shuffles, `spark.sql.shuffle.partitions`, `noop` writes for benchmarking |
| [notebook14](notebooks/notebook14.ipynb) | cluster | Caching and the Storage tab |
| [notebook15](notebooks/notebook15.ipynb) | cluster | Shuffle join vs. broadcast variables in a UDF, accumulators |
| [notebook16](notebooks/notebook16.ipynb) | cluster | Small × big join (sort-merge vs. `broadcast()` hint), big × big join |
| [notebook17](notebooks/notebook17.ipynb) | cluster | Inspecting how join output is distributed across partitions (data skew) |

[plain_python.py](notebooks/plain_python.py) is the notebook 1 workflow as a plain Python script.

## Datasets

The sample data lives in `./data` (paths below are as seen inside the containers):

| Path                                 | Contents |
|--------------------------------------|----------|
| `/home/spark/data/employee/1`        | Employee records (name, job title, salary, department ID, …) |
| `/home/spark/data/department/1`      | Departments (name, city, state, country) |
| `/home/spark/data/sales/1`           | Sales transactions (amount, retailer, city ID, timestamp) |
| `/home/spark/data/city/1`            | Cities (city, state, country) |

Some early notebooks build small DataFrames in memory or read paths created by earlier notebooks (e.g. `employees_tax`, `test`), so run them in order if a path is missing.