import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date = load_confs(spark)

@dlt.view
def compute_clusters_view():
  return (
    spark.read.table("main.centralized_system_tables.compute_clusters")
      .where(col("change_date") >= lit(load_start_date))
      .selectExpr(
        "cluster_id as clusterId",
        "cluster_name as clusterName",
        "create_time as createTime",
        "delete_time as deleteTime",
        "driver_node_type as driverNodeType",
        "worker_node_type as workerNodeType",
        "worker_count as workerCount",
        "min_autoscale_workers as minAutoscaleWorkers",
        "max_autoscale_workers as maxAutoscaleWorkers",
        "auto_termination_minutes as autoTerminationMinutes",
        "dbr_version as dbrVersion",
        "workspace_id as workspaceId",
        "account_id as accountId",
        "change_time as changeTime",
        "change_date as changeDate",
        "cluster_source as clusterSource"
      )
  )
