import dlt
from pyspark.sql.functions import *

load_start_date = '2025-03-06'

@dlt.view
def job_runs_view():
  return (spark.readStream.table("main.data_usage_log_summary.job_runs")
               .where(f"date>= '{load_start_date}'")
               .selectExpr("date", "workspaceId", "clusterId", "jobId", "runId",
                           "timestampadd(second, -durationSeconds, runTerminationTime) AS runStartTime",
                           "date_trunc('minute', timestampadd(second, -durationSeconds, runTerminationTime)) AS runStartTime_floor",
                           "date_add(MINUTE, 1, date_trunc('minute', runTerminationTime)) AS runTerminationTime_ceil",
                           "durationSeconds", "runTerminationTime", "runTerminalState", "runClusterType", "runTaskType", "runTriggerType"
               )
               )
  
@dlt.table
def filtered_job_runs():

  wlv = spark.read.table('workspaces_latest_view')

  return (spark.readStream.table("job_runs_view")
          .join(wlv, ["workspaceId"], "left_semi"))
  
@dlt.view
def billing_usage_temp_view():
  dt_values = [row['dt'] for row in spark.sql("select distinct cast(date as string) as dt from filtered_job_runs").collect() if row['dt'] is not None]

  return (spark.read.table("main.centralized_system_tables.billing_usage")
                          .selectExpr("usage_date as date", "account_id as accountId", "workspace_id as workspaceId", "usage_metadata.cluster_id AS clusterId",
                                      "sku_name as skuName", "billing_origin_product as usageOrigin")
                          .where(f"date>= '{load_start_date}'")
                          .where("clusterId is not null")
                          .distinct()
                      ).filter(col("date").isin(dt_values))
  
@dlt.view
def compute_clusters_temp_view():
  return (spark.read.table("integration.field_mfg_labs.dlt_silver_compute_clusters_latest")
                        .drop("changeTime")
                        )

@dlt.view
def driver_compute_timeline_temp_view():
  dt_values = [row['dt'] for row in spark.sql("select distinct cast(date as string) as dt from filtered_job_runs").collect() if row['dt'] is not None]\

  return (spark.read.table("main.centralized_system_tables.compute_node_timeline")
                                .where(f"cast(start_time as date)>= '{load_start_date}'")
                                .where("driver is true")
                                .selectExpr(
                                  "cast(start_time as date) as date",
                                  "account_id as accountId",
                                  "workspace_id as workspaceId",
                                  "cluster_id as clusterId",
                                  "start_time as start_time",
                                  "end_time as end_time",
                                  "cpu_user_percent as d_cpu_user_pct",
                                  "cpu_system_percent as d_cpu_system_pct",
                                  "cpu_wait_percent as d_cpu_wait_pct",
                                  "mem_used_percent as d_mem_used_pct",
                                  "mem_swap_percent as d_mem_swap_pct",
                                  "cast(network_received_bytes as decimal(38)) as d_network_received_bytes",
                                  "cast(network_sent_bytes as decimal(38)) as d_network_sent_bytes"
                                  )
                                ).filter(col("date").isin(dt_values))
  
@dlt.view
def worker_compute_timeline_temp_view():
  dt_values = [row['dt'] for row in spark.sql("select distinct cast(date as string) as dt from filtered_job_runs").collect() if row['dt'] is not None]

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
                                ).filter(col("date").isin(dt_values))
  
@dlt.view
def jr_bsug_df_temp_view():
  return spark.sql(f"""select jr.*, busg.usageOrigin, busg.skuName
                            from filtered_job_runs jr
                            left join billing_usage_temp_view busg
                            on jr.date = busg.date
                            and jr.workspaceId = busg.workspaceId
                            and jr.clusterId = busg.clusterId
                            """)
  
@dlt.view
def jr_bsug_cc_df_temp_view():
  return spark.sql(f"""
                    select jr.*, cc.autoTerminationMinutes
                    from jr_bsug_df_temp_view jr
                    left join compute_clusters_temp_view cc
                    on jr.workspaceId = cc.workspaceId and jr.clusterId = cc.clusterId
                    """)
  
@dlt.view
def jr_cnt_dr_df_temp_view():
  return spark.sql(f"""
                    select jr.*,
                    min(dr.d_cpu_user_pct) as min_d_cpu_user_pct, avg(dr.d_cpu_user_pct) as avg_d_cpu_user_pct, max(dr.d_cpu_user_pct) as max_d_cpu_user_pct,
                    min(dr.d_cpu_system_pct) as min_d_cpu_system_pct, avg(dr.d_cpu_system_pct) as avg_d_cpu_system_pct, max(dr.d_cpu_system_pct) as max_d_cpu_system_pct,
                    min(dr.d_cpu_wait_pct) as min_d_cpu_wait_pct, avg(dr.d_cpu_wait_pct) as avg_d_cpu_wait_pct, max(dr.d_cpu_wait_pct) as max_d_cpu_wait_pct,
                    min(dr.d_mem_used_pct) as min_d_mem_used_pct, avg(dr.d_mem_used_pct) as avg_d_mem_used_pct, max(dr.d_mem_used_pct) as max_d_mem_used_pct,
                    min(dr.d_mem_swap_pct) as min_d_mem_swap_pct, avg(dr.d_mem_swap_pct) as avg_d_mem_swap_pct, max(dr.d_mem_swap_pct) as max_d_mem_swap_pct,
                    sum(dr.d_network_received_bytes) as sum_d_network_received_bytes, sum(dr.d_network_sent_bytes) as sum_d_network_sent_bytes
                    from jr_bsug_cc_df_temp_view jr
                    left join driver_compute_timeline_temp_view dr
                    on jr.date = dr.date
                    and jr.workspaceid = dr.workspaceid
                    and jr.clusterId = dr.clusterId
                    where dr.start_time >= jr.runStartTime_floor and dr.end_time <= jr.runTerminationTime_ceil
                    group by all
                    """)

@dlt.table(
  name="dlt_silver_jobs_utilization",
  cluster_by=['date', 'workspaceId', 'clusterId', 'jobId']
)
def dlt_silver_jobs_utilization():
  return spark.sql(f"""
                    select jr.*,
                    min(dr.w_cpu_user_pct) as min_w_cpu_user_pct, avg(dr.w_cpu_user_pct) as avg_w_cpu_user_pct, max(dr.w_cpu_user_pct) as max_w_cpu_user_pct,
                    min(dr.w_cpu_system_pct) as min_w_cpu_system_pct, avg(dr.w_cpu_system_pct) as avg_w_cpu_system_pct, max(dr.w_cpu_system_pct) as max_w_cpu_system_pct,
                    min(dr.w_cpu_wait_pct) as min_w_cpu_wait_pct, avg(dr.w_cpu_wait_pct) as avg_w_cpu_wait_pct, max(dr.w_cpu_wait_pct) as max_w_cpu_wait_pct,
                    min(dr.w_mem_used_pct) as min_w_mem_used_pct, avg(dr.w_mem_used_pct) as avg_w_mem_used_pct, max(dr.w_mem_used_pct) as max_w_mem_used_pct,
                    min(dr.w_mem_swap_pct) as min_w_mem_swap_pct, avg(dr.w_mem_swap_pct) as avg_w_mem_swap_pct, max(dr.w_mem_swap_pct) as max_w_mem_swap_pct,
                    sum(dr.w_network_received_bytes) as sum_w_network_received_bytes, sum(dr.w_network_sent_bytes) as sum_w_network_sent_bytes
                    from jr_cnt_dr_df_temp_view jr
                    left join worker_compute_timeline_temp_view dr
                    on jr.date = dr.date
                    and jr.workspaceid = dr.workspaceid
                    and jr.clusterId = dr.clusterId
                    where dr.start_time >= jr.runStartTime_floor and dr.end_time <= jr.runTerminationTime_ceil
                    group by all
                    """)

  