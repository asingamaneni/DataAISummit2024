from datetime import timedelta, datetime

from brickflow import (
    ctx,
    Cluster,
    Workflow,
    JarTaskLibrary,
)

# type: ignore


wf = Workflow(
    "coffeeco_dais2024_wf",
    default_cluster=Cluster(
        name="ashok-singamaneni_dais2024",
        spark_version="13.3.x-scala2.12",
        node_type_id="mgd-fleet.xlarge",
        min_workers=1,
        max_workers=16,
        driver_node_type_id="mg-fleet.xlarge",
        custom_tags={
            "silly_tags": "silly_values",
        },
        policy_id="YOUR_POLICY_ID",
    ),
    # Optional parameters below
    schedule_quartz_expression=None,
    tags={
        "product_id": "dais2024_demo",
        "slack_channel": "brickflow-support",
    },
    common_task_parameters={
        "catalog": "coffeeco",
        "tin_database": "tin",
        "bronze_database": "bronze",
        "silver_database": "silver",
        "gold_database": "gold",
        "date": datetime.now().strftime("%Y-%m-%d"),
        "tomorrow_date": (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
    },
    libraries=[
        JarTaskLibrary(
            jar="dbfs:/kafka-jars/databricks-shaded-strimzi-kafka-oauth-client-1.1.jar"
        )
    ]
)


@wf.task()
def start():
    pass

ingestion_tables = ["coffee", "coffee_product", "customer", "product", "store", "order", "total"]
tasks_list = []


def create_dynamic_tasks(table):  # type: ignore
    # create ingestion tasks for coffee
    @wf.task(name=f"tin_{table}", depends_on=start)
    def tin_ingest():
        pass

    @wf.task(name=f"bronze_{table}", depends_on=f"tin_{table}")
    def bronze_ingest():
        pass

    @wf.task(name=f"silver_{table}", depends_on=f"bronze_{table}")
    def silver_ingest():
        pass

    tasks_list.append(f"silver_{table}")

    return tin_ingest, bronze_ingest, silver_ingest


for table in ingestion_tables:
    create_dynamic_tasks(table)


@wf.task(depends_on=tasks_list)
def branch():
    pass


@wf.task(depends_on=branch)
def gold_denormalized():
    pass


@wf.task(depends_on=[gold_denormalized])
def end():
    pass
