ADD JAR ${hivevar:opencsv_jar};
ADD JAR ${hivevar:hcatalog_jar};

USE default;

DROP TABLE IF EXISTS mr_park_visits;
CREATE EXTERNAL TABLE mr_park_visits (
  park_id STRING,
  \`date\` STRING,
  visits_count BIGINT,
  avg_group_size DOUBLE,
  sum_group_size BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir3}';

DROP TABLE IF EXISTS parks_ext;
CREATE EXTERNAL TABLE parks_ext (
  park_id STRING,
  name STRING,
  region STRING,
  facilities STRING,
  attractions STRING,
  nature_types STRING,
  established_year STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar" = "\"",
  "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir4}'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS project1_result;
CREATE EXTERNAL TABLE project1_result (
  nature_type STRING,
  region STRING,
  total_visits BIGINT,
  avg_group_size DOUBLE,
  visits_deviation DOUBLE
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hivevar:output_dir6}';

WITH joined AS (
  SELECT
    p.region AS region,
    p.nature_types AS nature_types,
    v.visits_count AS visits_count,
    v.sum_group_size AS sum_group_size
  FROM mr_park_visits v
  JOIN parks_ext p
    ON v.park_id = p.park_id
),
exploded AS (
  SELECT
    j.region AS region,
    TRIM(nt.nature_type) AS nature_type,
    j.visits_count AS visits_count,
    j.sum_group_size AS sum_group_size
  FROM joined j
  LATERAL VIEW EXPLODE(SPLIT(j.nature_types, ';')) nt AS nature_type
),
aggregated AS (
  SELECT
    nature_type,
    region,
    SUM(visits_count) AS total_visits,
    SUM(sum_group_size) / SUM(visits_count) AS avg_group_size
  FROM exploded
  WHERE nature_type <> ''
  GROUP BY nature_type, region
)
INSERT OVERWRITE TABLE project1_result
SELECT
  nature_type,
  region,
  total_visits,
  avg_group_size,
  total_visits - AVG(total_visits) OVER (PARTITION BY nature_type) AS visits_deviation
FROM aggregated;
