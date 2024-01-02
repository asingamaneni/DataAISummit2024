from brickflow import Workflow, Task, Cluster, WorkflowEmailNotifications, ctx
from brickflow_plugins import BashOperator
import requests

# fmt: off
iso_country_codes = ["US", "CA", "MX", "BR", "CL", "CO"]
# fmt: on

wf = Workflow(
    f"brickflow-summit-demo",
    default_cluster=Cluster.from_existing_cluster("your_cluster_name"),
    # Optional parameters below
    schedule_quartz_expression=None,
    tags={
        "product_id": "brickflow_demo",
        "slack_channel": "your_slack_channel_name",
    },
    common_task_parameters={
        "catalog": "demo_catalog",
        "database": "demo_db",
    },
    enable_plugins=True,
)


@wf.task()
def start():
    pass


tasks_list = []
ingest_table_names = []
conform_table_names = []
database = f"{ctx.get_parameter(key='catalog')}.{ctx.get_parameter(key='database')}"


def create_dynamic_tasks(country):
    transactions_task_name = f"transactions_ingest_{country}"
    store_task_name = f"stores_ingest_{country}"
    conform_task_name = f"conform_{country}"

    transactions_table_name = (
        f"{database}.{ctx.get_parameter(key='brickflow_env')}_{transactions_task_name}"
    )
    stores_table_name = (
        f"{database}.{ctx.get_parameter(key='brickflow_env')}_{store_task_name}"
    )
    conform_table_name = (
        f"{database}.{ctx.get_parameter(key='brickflow_env')}_{conform_task_name}"
    )
    ingest_table_names.append(transactions_table_name)
    ingest_table_names.append(stores_table_name)
    conform_table_names.append(conform_table_name)

    @wf.task(name=transactions_task_name, depends_on=start)
    def transactions_ingest():
        df = ctx.spark.read.csv(
            "dbfs:/tmp/dataaisummit/transactions.csv", header=True, inferSchema=True
        )
        df.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(transactions_table_name)

    @wf.task(name=store_task_name, depends_on=transactions_task_name)
    def stores_ingest():
        df = ctx.spark.read.csv(
            "dbfs:/tmp/dataaisummit/stores.csv", header=True, inferSchema=True
        )
        df.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(stores_table_name)

    @wf.task(name=conform_task_name, depends_on=store_task_name)
    def conform():
        conform_sql = f"""
            select s.store_nbr, s.city, s.type, t.`date`, t.transactions , '{country}' as country
            from {stores_table_name} s
            join {transactions_table_name} t
            on cast(s.store_nbr as bigint)=cast(t.store_nbr as bigint)
        """
        ctx.log.info(f"conform_sql: {conform_sql}")
        df = ctx.spark.sql(conform_sql)
        df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).saveAsTable(conform_table_name)

    tasks_list.append(conform_task_name)

    return transactions_ingest, stores_ingest, conform


for country in iso_country_codes:
    create_dynamic_tasks(country)


@wf.task(depends_on=tasks_list)
def deliver():
    union_sql = " UNION ALL ".join(
        [f"select * from {table}" for table in conform_table_names]
    )
    df = ctx.spark.sql(union_sql)
    df.write.format("delta").mode("overwrite").option(
        "mergeSchema", "true"
    ).saveAsTable(f"{database}.{ctx.get_parameter(key='brickflow_env')}_deliver")


@wf.task(depends_on=deliver)
def business_layer():
    business_layer_sql = f"""
        select country, store_nbr, sum(`transactions`) as total_transactions
        from {database}.{ctx.get_parameter(key='brickflow_env')}_deliver
        group by country, store_nbr
    """
    print(business_layer_sql)
    df = ctx.spark.sql(business_layer_sql)
    df.write.format("delta").mode("overwrite").option(
        "mergeSchema", "true"
    ).saveAsTable(f"{database}.{ctx.get_parameter(key='brickflow_env')}_business_layer")


@wf.task(depends_on=deliver)
def drop_ingest_tables():
    for table in ingest_table_names:
        ctx.spark.sql(f"drop table {table}")
    for table in conform_table_names:
        ctx.spark.sql(f"drop table {table}")


@wf.task(depends_on=[drop_ingest_tables, business_layer])
def end():
    pass
