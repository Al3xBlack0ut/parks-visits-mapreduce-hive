# Park Visits Analytics with Hadoop MapReduce, Apache Hive and Airflow

A batch data-processing pipeline that aggregates synthetic national-park visit records with **Python Hadoop Streaming**, then enriches and analyses the results in **Apache Hive**. An **Apache Airflow** DAG orchestrates the HDFS cleanup, MapReduce job, Hive aggregation and result extraction.

## Pipeline overview

```text
Synthetic CSV visits                         Parks dimension CSV
        |                                             |
        v                                             |
Python Mapper                                      |
        |                                           |
        v                                           |
Combiner (sum group sizes and visit counts)          |
        |                                           |
        v                                           |
Reducer (daily park-level aggregates)                |
        |                                           |
        +--------------------+----------------------+
                             |
                             v
                    Apache Hive join
                             |
              EXPLODE nature_types and aggregate
                             |
                  Window-based deviation
                             |
                             v
                     JSON result in HDFS
```

## What it calculates

### Stage 1 - Hadoop MapReduce

For each `(park_id, date)` pair, the mapper extracts the visit date from `entry_time` and emits the group size and a visit counter. The combiner and reducer sum these distributive components. The reducer outputs:

- `park_id`
- `date`
- `visits_count`
- `avg_group_size`
- `sum_group_size` (retained so the Hive stage can calculate a correctly weighted average)

The job uses a Python mapper, combiner and reducer through Hadoop Streaming.

### Stage 2 - Apache Hive

Hive joins the daily aggregates with the parks dimension table, splits multi-valued `nature_types` using `LATERAL VIEW EXPLODE`, and groups the data by nature type and region. It calculates total visits, a weighted average group size and the difference between a region's visit count and the average for that nature type across represented regions using a window function.

The final output contains `nature_type`, `region`, `total_visits`, `avg_group_size` and `visits_deviation`.

## Technologies

- Python 3: Hadoop Streaming mapper, combiner and reducer
- Apache Hadoop: HDFS and MapReduce Streaming
- Apache Hive: external tables, joins, `LATERAL VIEW EXPLODE`, aggregations and window functions
- Apache Airflow: workflow orchestration with BashOperator
- CSV and JSON data formats
- Bash

## Repository structure

```text
.
├── airflow/
│   └── dags/
│       └── parks_visits_dag.py
├── input/
│   └── sample/
│       ├── datasource1/
│       │   └── visits_sample.csv
│       └── datasource4/
│           └── parks.csv
├── project_files/
│   ├── mapper.py
│   ├── combiner.py
│   ├── reducer.py
│   ├── hive.hql
│   ├── run_mr.sh
│   └── run_hive.sh
└── tests/
    ├── fixtures/
    │   └── visits.csv
    └── test_mapreduce.py
```

## Data

The source datasets were synthetically generated with [Datafaker](https://www.datafaker.net/). The repository contains a small visit sample and the parks dimension table so the schemas and processing logic are easy to inspect. The complete multi-file visit dataset is intentionally not included in this portfolio repository.

`datasource1` contains visit records with `visit_id`, `park_id`, `visitor_id`, `entry_time`, `exit_time`, `group_size` and `activity`.

`datasource4` contains park metadata, including `park_id`, `region` and the semicolon-separated `nature_types` field.

## Requirements

The distributed pipeline expects an environment with:

- Apache Hadoop with HDFS and the `mapred streaming` command available
- Apache Hive with HiveServer2/Beeline
- OpenCSV and Hive HCatalog libraries available to Hive
- Apache Airflow with the standard provider installed (for the DAG)
- Python 3 on Hadoop worker nodes

The Airflow DAG uses the Airflow 3-style `airflow.sdk.Param` interface. Hadoop, Hive and Airflow must be configured in the execution environment; this repository does not bundle those services.

## Run the MapReduce stage

Copy the sample input to HDFS. The directory layout expected by the DAG and the scripts is:

```text
/project1/input/datasource1
/project1/input/datasource4
```

For example:

```bash
hadoop fs -mkdir -p /project1/input/datasource1 /project1/input/datasource4
hadoop fs -put input/sample/datasource1/visits_sample.csv /project1/input/datasource1/
hadoop fs -put input/sample/datasource4/parks.csv /project1/input/datasource4/
```

Run Hadoop Streaming:

```bash
bash project_files/run_mr.sh /project1/input/datasource1 /project1/output_mr3
```

## Run the Hive stage

After MapReduce has completed:

```bash
bash project_files/run_hive.sh \
  /project1/output_mr3 \
  /project1/input/datasource4 \
  /project1/output6
```

The Hive script writes the final JSON output to the configured HDFS location.

## Run with Airflow

Place `airflow/dags/parks_visits_dag.py` in the Airflow DAGs directory and copy `project_files/` into a sibling directory named `project_files` inside that DAGs directory. The default DAG parameters assume:

- `project_files_dir`: `/opt/airflow/dags/project_files`
- `input_dir`: `/project1/input`
- `output_mr_dir`: `/project1/output_mr3`
- `output_dir`: `/project1/output6`

Override the DAG parameters when your environment uses different paths. The Airflow workers must be able to call Hadoop and Beeline and reach the configured HDFS and Hive services.

## Tests

The MapReduce components can be smoke-tested locally without Hadoop or Hive:

```bash
python -m unittest discover -s tests -v
```

The test runs the mapper, combiner and reducer over a small CSV fixture and checks the daily park aggregates. It does not execute the distributed Hadoop job or the Hive query.

## Scope

This repository focuses on the data-processing implementation. Course handouts, grading rubrics, IDE metadata, generated bytecode, intermediate result dumps and the full 100+ MB source dataset are not included.
