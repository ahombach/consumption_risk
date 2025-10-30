import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date = load_confs(spark)

@dlt.view
def compute_timeline_view():

  return (spark.read.table("main.centralized_system_tables.compute_node_timeline")
                                .where(col("start_time").cast("date") >= lit(load_start_date))
                                .selectExpr(
                                  "cast(start_time as date) as date",
                                  "account_id as accountId",
                                  "workspace_id as workspaceId",
                                  "cluster_id as clusterId",
                                  "start_time as start_time",
                                  "end_time as end_time",
                                  "cpu_user_percent as cpu_user_pct",
                                  "cpu_system_percent as cpu_system_pct",
                                  "cpu_wait_percent as cpu_wait_pct",
                                  "mem_used_percent as mem_used_pct",
                                  "mem_swap_percent as mem_swap_pct",
                                  "cast(network_received_bytes as decimal(38)) as network_received_bytes",
                                  "cast(network_sent_bytes as decimal(38)) as network_sent_bytes",
                                  "driver"
                                  )
                                )