from datetime import datetime

from airflow import DAG
from airflow.sdk import Param
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="parks-visits-pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "project_files_dir": Param("/opt/airflow/dags/project_files", type="string"),
        "input_dir": Param("/project1/input", type="string"),
        "output_mr_dir": Param("/project1/output_mr3", type="string"),
        "output_dir": Param("/project1/output6", type="string"),
    },
    render_template_as_native_obj=True,
) as dag:
    clean_output_mr_dir = BashOperator(
        task_id="clean_output_mr_dir",
        bash_command=(
            "if hadoop fs -test -d {{ params.output_mr_dir }}; "
            "then hadoop fs -rm -f -r {{ params.output_mr_dir }}; fi"
        ),
    )

    clean_output_dir = BashOperator(
        task_id="clean_output_dir",
        bash_command=(
            "if hadoop fs -test -d {{ params.output_dir }}; "
            "then hadoop fs -rm -f -r {{ params.output_dir }}; fi"
        ),
    )

    mapreduce_streaming = BashOperator(
        task_id="mapreduce_streaming",
        bash_command=(
            "mapred streaming "
            "-D stream.map.output.field.separator=$'\\t' "
            "-D stream.num.map.output.key.fields=2 "
            "-D stream.num.reduce.output.key.fields=2 "
            "-files {{ params.project_files_dir }}/mapper.py,"
            "{{ params.project_files_dir }}/combiner.py,"
            "{{ params.project_files_dir }}/reducer.py "
            "-mapper \"python mapper.py\" "
            "-combiner \"python combiner.py\" "
            "-reducer \"python reducer.py\" "
            "-input {{ params.input_dir }}/datasource1 "
            "-output {{ params.output_mr_dir }}"
        ),
    )

    hive = BashOperator(
        task_id="hive_aggregation",
        bash_command=(
            "if [ -x /opt/hive/bin/beeline ]; then BEELINE=/opt/hive/bin/beeline; "
            "else BEELINE=beeline; fi; "
            "find_jar() { for dir in /opt/hive/lib /usr/lib/hive/lib; do "
            "if compgen -G \"$dir/$1\" > /dev/null; then "
            "compgen -G \"$dir/$1\" | head -n 1; return 0; fi; done; return 1; }; "
            "OPENCSV_JAR=$(find_jar 'opencsv-*.jar'); "
            "HCATALOG_JAR=$(find_jar 'hive-hcatalog-core-*.jar'); "
            "test -n \"$OPENCSV_JAR\" && test -n \"$HCATALOG_JAR\"; "
            "\"$BEELINE\" -n \"$(id -un)\" "
            "-u \"jdbc:hive2://localhost:10000/default\" "
            "--hivevar input_dir3=\"{{ params.output_mr_dir }}\" "
            "--hivevar input_dir4=\"{{ params.input_dir }}/datasource4\" "
            "--hivevar output_dir6=\"{{ params.output_dir }}\" "
            "--hivevar opencsv_jar=\"$OPENCSV_JAR\" "
            "--hivevar hcatalog_jar=\"$HCATALOG_JAR\" "
            "-f \"{{ params.project_files_dir }}/hive.hql\""
        ),
    )

    get_output = BashOperator(
        task_id="get_output",
        bash_command=(
            "hadoop fs -getmerge {{ params.output_dir }} output6.json && "
            "head output6.json"
        ),
    )

    [clean_output_mr_dir, clean_output_dir] >> mapreduce_streaming
    mapreduce_streaming >> hive >> get_output
