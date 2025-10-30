import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date, change_date = load_confs(spark)

@dlt.view
def worker_compute_timeline_temp_view():

  return (spark.read.table("main.centralized_system_tables.compute_node_timeline")
                                .where(f"cast(start_time as date)>= '{load_start_date}'")
                                .where("driver is false")
                                .selectExpr(
                                  "cast(start_time as date) as date",
                                  "account_id as accountId",
                                  "workspace_id as workspaceId",
                                  "cluster_id as clusterId",
                                  "instance_id as instanceId",
                                  "start_time as start_time",
                                  "end_time as end_time",
                                  "cpu_user_percent as w_cpu_user_pct",
                                  "cpu_system_percent as w_cpu_system_pct",
                                  "cpu_wait_percent as w_cpu_wait_pct",
                                  "mem_used_percent as w_mem_used_pct",
                                  "mem_swap_percent as w_mem_swap_pct",
                                  "cast(network_received_bytes as decimal(38)) as w_network_received_bytes",
                                  "cast(network_sent_bytes as decimal(38)) as w_network_sent_bytes"
                                  )
                                )