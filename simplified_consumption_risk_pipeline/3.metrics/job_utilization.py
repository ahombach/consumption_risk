import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date = load_confs(spark)
mem_threshold = 50
cpu_threshold = 30

@dlt.view
def job_runs_cluster_utilization_view():
    jrfv = spark.read.table("job_runs_filtered_view")
    cfv = spark.read.table("compute_clusters_filtered_view")
    ctv = spark.read.table("compute_timeline_view")
    wiv = spark.read.table("workload_insights_view").where(col("runId").isNotNull())

    jrwi = (
      wiv.join(
            jrfv,
            ["date", "workspaceId", "clusterId", "jobId", "runId"],
            "inner"
        )
    )
    #join the driver timeline information into the combined table
    
    #rename columns to avoid join conflicts and filter down to only drivers
    ctvr = ctv.withColumnRenamed("workspaceId", "ctv_workspaceId") \
             .withColumnRenamed("clusterId", "ctv_clusterId") \
             .withColumnRenamed("date", "ctv_date")

    dtv = ctvr.where("driver is true")
    wtv = ctvr.where("driver is false")     

    #conditions for driver join
    driver_cond = (
        (jrwi["date"] == dtv["ctv_date"]) &
        (jrwi["workspaceId"] == dtv["ctv_workspaceId"]) &
        (jrwi["clusterId"] == dtv["ctv_clusterId"]) &
        (dtv["start_time"] >= jrwi["runStartTime_floor"]) &
        (dtv["end_time"] <= jrwi["runTerminationTime_ceil"])
    )
    #join the driver timeline information into the combined table
    jrdt = jrwi.join(dtv, driver_cond, "left")

    #set aggregate expressions for driver calculations
    grouping_cols = [c for c in jrwi.columns]
    agg_exprs_driver = [
        min("cpu_user_pct").alias("min_d_cpu_user_pct"),
        avg("cpu_user_pct").alias("avg_d_cpu_user_pct"),
        max("cpu_user_pct").alias("max_d_cpu_user_pct"),
        min("cpu_system_pct").alias("min_d_cpu_system_pct"),
        avg("cpu_system_pct").alias("avg_d_cpu_system_pct"),
        max("cpu_system_pct").alias("max_d_cpu_system_pct"),
        min("cpu_wait_pct").alias("min_d_cpu_wait_pct"),
        avg("cpu_wait_pct").alias("avg_d_cpu_wait_pct"),
        max("cpu_wait_pct").alias("max_d_cpu_wait_pct"),
        min("mem_used_pct").alias("min_d_mem_used_pct"),
        avg("mem_used_pct").alias("avg_d_mem_used_pct"),
        max("mem_used_pct").alias("max_d_mem_used_pct"),
        min("mem_swap_pct").alias("min_d_mem_swap_pct"),
        avg("mem_swap_pct").alias("avg_d_mem_swap_pct"),
        max("mem_swap_pct").alias("max_d_mem_swap_pct"),
        sum("network_received_bytes").alias("sum_d_network_received_bytes"),
        sum("network_sent_bytes").alias("sum_d_network_sent_bytes")
    ]

    jrdta = (
        jrdt.drop("ctv_date", "ctv_workspaceId", "ctv_clusterId")
            .groupBy(*grouping_cols)
            .agg(*agg_exprs_driver)
    )
  
    #join the worker timeline information into the combined table
    jrdta_alias = jrdta.alias("jr")
    wtv_alias = wtv.alias("wt")

    worker_cond = (
        (col("jr.date") == col("wt.ctv_date")) &
        (col("jr.workspaceId") == col("wt.ctv_workspaceId")) &
        (col("jr.clusterId") == col("wt.ctv_clusterId")) &
        (col("wt.start_time") >= col("jr.runStartTime_floor")) &
        (col("wt.end_time") <= col("jr.runTerminationTime_ceil"))
    )

    jrwt = jrdta_alias.join(wtv_alias, worker_cond, "left")

    #set aggregate expressions for worker calculations
    grouping_cols_final = [c for c in jrdta.columns]
    agg_exprs_worker = [
        min("cpu_user_pct").alias("min_w_cpu_user_pct"),
        avg("cpu_user_pct").alias("avg_w_cpu_user_pct"),
        max("cpu_user_pct").alias("max_w_cpu_user_pct"),
        min("cpu_system_pct").alias("min_w_cpu_system_pct"),
        avg("cpu_system_pct").alias("avg_w_cpu_system_pct"),
        max("cpu_system_pct").alias("max_w_cpu_system_pct"),
        min("cpu_wait_pct").alias("min_w_cpu_wait_pct"),
        avg("cpu_wait_pct").alias("avg_w_cpu_wait_pct"),
        max("cpu_wait_pct").alias("max_w_cpu_wait_pct"),
        min("mem_used_pct").alias("min_w_mem_used_pct"),
        avg("mem_used_pct").alias("avg_w_mem_used_pct"),
        max("mem_used_pct").alias("max_w_mem_used_pct"),
        min("mem_swap_pct").alias("min_w_mem_swap_pct"),
        avg("mem_swap_pct").alias("avg_w_mem_swap_pct"),
        max("mem_swap_pct").alias("max_w_mem_swap_pct"),
        sum("network_received_bytes").alias("sum_w_network_received_bytes"),
        sum("network_sent_bytes").alias("sum_w_network_sent_bytes")
    ]
    
    jrwta = (
        jrwt.drop("ctv_date", "ctv_workspaceId", "ctv_clusterId")
            .groupBy(*grouping_cols_final)
            .agg(*agg_exprs_worker)
    )

    return jrwta

@dlt.view
def job_runs_utilization_aug_view(): 
    jruv = spark.read.table("job_runs_cluster_utilization_view")
    
    return (jruv.withColumn(
            "avg_worker_cpu_utilization",
            col("avg_w_cpu_user_pct") + col("avg_w_cpu_system_pct"),
        )
        .withColumn(
            "avg_driver_cpu_utilization",
            col("avg_d_cpu_user_pct") + col("avg_d_cpu_system_pct"),
        )
        .withColumn(
            "driver_dbu_percentage",
            col("attributedDriverDbus")
            / (col("attributedDriverDbus") + col("attributedWorkerDbus")),
        )
        .withColumn(
            "worker_dbu_percentage",
            col("attributedWorkerDbus")
            / (col("attributedDriverDbus") + col("attributedWorkerDbus")),
        )
        .withColumn(
            "worker_weighted_cpu_utilization",
            col("avg_worker_cpu_utilization") * col("worker_dbu_percentage"),
        )
        .withColumn(
            "driver_weighted_cpu_utilization",
            col("avg_driver_cpu_utilization") * col("driver_dbu_percentage"),
        )
        .withColumn(
            "avg_cluster_cpu_utilization",
            col("avg_worker_cpu_utilization") * col("worker_dbu_percentage")
            + col("avg_driver_cpu_utilization") * col("driver_dbu_percentage"),
        )
        .withColumn(
            "avg_cluster_mem_used_percent",
            col("avg_w_mem_used_pct") * col("worker_dbu_percentage")
            + col("avg_d_mem_used_pct") * col("driver_dbu_percentage"),
        )
        .withColumn(
            "avg_mem_swap_used_percent",
            col("avg_w_mem_swap_pct") * col("worker_dbu_percentage")
            + col("avg_d_mem_swap_pct") * col("driver_dbu_percentage"),
        )
        .withColumn(
            "avg_cpu_wait_percent",
            col("avg_w_cpu_wait_pct") * col("worker_dbu_percentage")
            + col("avg_d_cpu_wait_pct") * col("driver_dbu_percentage"),
        )
        .withColumn(
            "total_network_gb_received",
            (col("sum_d_network_received_bytes") + col("sum_w_network_received_bytes"))
            / lit(1000000000),
        )
        .withColumn(
            "total_network_gb_sent",
            (col("sum_d_network_sent_bytes") + col("sum_w_network_sent_bytes"))
            / lit(1000000000),
        )
        .withColumn(
            "utilizationPenalty",
            (
                (
                    greatest(
                        lit(cpu_threshold) - col("avg_cluster_cpu_utilization"),
                        lit(0),
                    )
                    / lit(cpu_threshold)
                )
                + (
                    greatest(
                        lit(mem_threshold) - col("avg_cluster_mem_used_percent"),
                        lit(0),
                    )
                    / lit(mem_threshold)
                )
            )/2,
        )
        .withColumn(
            "revAtRiskUtilization",
            col("attributedRevenue") * col("utilizationPenalty")
            )
        )