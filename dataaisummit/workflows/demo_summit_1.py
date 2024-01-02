from brickflow import Workflow, Task, Cluster, WorkflowEmailNotifications, ctx
from brickflow_plugins import BashOperator
import requests

# fmt: off
iso_country_codes = ["US", "CA", "MX", "BR", "CL", "CO"]
# fmt: on

job_cluster_arn = "arn:aws:iam::XXXXXXXXXXX:instance-profile/your_profile_name"
job_cluster_policy_id = "your_policy_id"


def createJobCluster(name: str):
    aws_config = {
        "first_on_demand": 1,
        "availability": "SPOT_WITH_FALLBACK",
        "instance_profile_arn": job_cluster_arn,
        "spot_bid_price_percent": 100,
        "ebs_volume_type": "GENERAL_PURPOSE_SSD",
        "ebs_volume_count": 3,
        "ebs_volume_size": 100,
    }

    return Cluster(
        name=name,
        spark_version="13.3.x-scala2.12",
        node_type_id="m6gd.2xlarge",
        driver_node_type_id="m6gd.2xlarge",
        min_workers=1,
        max_workers=6,
        enable_elastic_disk=True,
        policy_id=job_cluster_policy_id,
        aws_attributes=aws_config,
        custom_tags={"product_id": "brickflow_demo"},
    )


if ctx.env == "prod":
    cluster = createJobCluster("brickflow-summit-demo_prod")
elif ctx.env == "dev":
    cluster = createJobCluster("brickflow-summit-demo_dev")
elif ctx.env == "qa":
    cluster = createJobCluster("brickflow-summit-demo_qa")
else:
    cluster = createJobCluster("brickflow-summit-demo_local")


def create_dynamic_workflows(country):
    wf = Workflow(
        f"brickflow-summit-demo-{country}",
        default_cluster=cluster,
        # Optional parameters below
        schedule_quartz_expression=None,
        tags={
            "product_id": "brickflow_demo",
            "slack_channel": "your_slack_channel_name",
        },
        enable_plugins=True,
    )

    @wf.task()
    def start():
        pass

    @wf.task(depends_on=start)
    def end():
        pass

    return wf


workflows = []

for country in iso_country_codes:
    workflows.append(create_dynamic_workflows(country=country))
