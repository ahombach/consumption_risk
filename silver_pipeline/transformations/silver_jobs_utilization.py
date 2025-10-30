import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date, change_date = load_confs(spark)

@dlt.view
def job_runs():
    wlv = spark.read.table('workspaces_latest_view')
    return (
        spark.read.table("job_runs_view")
        .join(wlv, ["workspaceId"], "left_semi")
    )

@dlt.view
def jr_busg_view():
    jr = spark.read.table("job_runs")
    busg = spark.read.table("billing_usage_view")
    return (
        jr.join(
            busg,
            on=["date", "workspaceId", "clusterId"],
            how="left"
        )
        .select(jr["*"], busg.usageOrigin, busg.skuName)
    )

@dlt.view
def jr_busg_cc_view():
    jr = spark.read.table("jr_busg_view")
    cc = spark.read.table("dlt_silver_compute_clusters").drop("changeTime")
    return (
        jr.join(
            cc,
            on=['workspaceId', 'clusterId'],
            how="left"
        )
        .select(jr["*"], cc.autoTerminationMinutes)
    )

@dlt.view
def jr_cnt_dr_view():
    jr = spark.read.table("jr_busg_cc_view")
    dr = spark.read.table("driver_compute_timeline_temp_view")
    dr_renamed = dr.withColumnRenamed("workspaceId", "dr_workspaceId") \
                   .withColumnRenamed("clusterId", "dr_clusterId") \
                   .withColumnRenamed("date", "dr_date")
    cond = (
        (jr["date"] == dr_renamed["dr_date"]) &
        (jr["workspaceId"] == dr_renamed["dr_workspaceId"]) &
        (jr["clusterId"] == dr_renamed["dr_clusterId"]) &
        (dr_renamed["start_time"] >= jr["runStartTime_floor"]) &
        (dr_renamed["end_time"] <= jr["runTerminationTime_ceil"])
    )
    joined = jr.join(dr_renamed, cond, "left")
    group_cols = [col for col in jr.columns]
    agg_exprs = [
        min("d_cpu_user_pct").alias("min_d_cpu_user_pct"),
        avg("d_cpu_user_pct").alias("avg_d_cpu_user_pct"),
        max("d_cpu_user_pct").alias("max_d_cpu_user_pct"),
        min("d_cpu_system_pct").alias("min_d_cpu_system_pct"),
        avg("d_cpu_system_pct").alias("avg_d_cpu_system_pct"),
        max("d_cpu_system_pct").alias("max_d_cpu_system_pct"),
        min("d_cpu_wait_pct").alias("min_d_cpu_wait_pct"),
        avg("d_cpu_wait_pct").alias("avg_d_cpu_wait_pct"),
        max("d_cpu_wait_pct").alias("max_d_cpu_wait_pct"),
        min("d_mem_used_pct").alias("min_d_mem_used_pct"),
        avg("d_mem_used_pct").alias("avg_d_mem_used_pct"),
        max("d_mem_used_pct").alias("max_d_mem_used_pct"),
        min("d_mem_swap_pct").alias("min_d_mem_swap_pct"),
        avg("d_mem_swap_pct").alias("avg_d_mem_swap_pct"),
        max("d_mem_swap_pct").alias("max_d_mem_swap_pct"),
        sum("d_network_received_bytes").alias("sum_d_network_received_bytes"),
        sum("d_network_sent_bytes").alias("sum_d_network_sent_bytes"),
    ]
    return joined.groupBy(*group_cols).agg(*agg_exprs).drop("dr_date").drop( "dr_workspaceId").drop("dr_clusterId")

@dlt.table(
    name="dlt_silver_jobs_utilization",
    cluster_by=['date', 'workspaceId', 'clusterId', 'jobId']
)
def dlt_silver_jobs_utilization():
    jr = spark.read.table("jr_cnt_dr_view")
    dr = spark.read.table("worker_compute_timeline_temp_view")
    dr_renamed = dr.withColumnRenamed("workspaceId", "dr_workspaceId") \
                   .withColumnRenamed("clusterId", "dr_clusterId") \
                   .withColumnRenamed("date", "dr_date")
    cond = (
        (jr["date"] == dr_renamed["dr_date"]) &
        (jr["workspaceId"] == dr_renamed["dr_workspaceId"]) &
        (jr["clusterId"] == dr_renamed["dr_clusterId"]) &
        (dr_renamed["start_time"] >= jr["runStartTime_floor"]) &
        (dr_renamed["end_time"] <= jr["runTerminationTime_ceil"])
    )
    joined = jr.join(dr_renamed, cond, "left")
    group_cols = [col for col in jr.columns]
    agg_exprs = [
        min("w_cpu_user_pct").alias("min_w_cpu_user_pct"),
        avg("w_cpu_user_pct").alias("avg_w_cpu_user_pct"),
        max("w_cpu_user_pct").alias("max_w_cpu_user_pct"),
        min("w_cpu_system_pct").alias("min_w_cpu_system_pct"),
        avg("w_cpu_system_pct").alias("avg_w_cpu_system_pct"),
        max("w_cpu_system_pct").alias("max_w_cpu_system_pct"),
        min("w_cpu_wait_pct").alias("min_w_cpu_wait_pct"),
        avg("w_cpu_wait_pct").alias("avg_w_cpu_wait_pct"),
        max("w_cpu_wait_pct").alias("max_w_cpu_wait_pct"),
        min("w_mem_used_pct").alias("min_w_mem_used_pct"),
        avg("w_mem_used_pct").alias("avg_w_mem_used_pct"),
        max("w_mem_used_pct").alias("max_w_mem_used_pct"),
        min("w_mem_swap_pct").alias("min_w_mem_swap_pct"),
        avg("w_mem_swap_pct").alias("avg_w_mem_swap_pct"),
        max("w_mem_swap_pct").alias("max_w_mem_swap_pct"),
        sum("w_network_received_bytes").alias("sum_w_network_received_bytes"),
        sum("w_network_sent_bytes").alias("sum_w_network_sent_bytes"),
    ]
    return joined.groupBy(*group_cols).agg(*agg_exprs)\
      .drop('dr_date')\
      .drop('dr_workspaceId')\
      .drop('dr_clusterId')