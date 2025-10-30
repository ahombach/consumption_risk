import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date = load_confs(spark)

@dlt.table
def job_runs_metrics():
    df_job_runs_interactive = spark.read.table("job_runs_interactive").drop('attributedRevenue', 'productType')
    df_job_runs_cluster_utilization = spark.read.table("job_runs_utilization_aug_view")
    df_job_runs_failures = spark.read.table("job_runs_failures").drop('attributedRevenue', 'workloadStatus')

    return df_job_runs_interactive.join(df_job_runs_cluster_utilization, ["date", "workspaceId", "clusterId", "runId", "jobId"], "inner")\
        .join(df_job_runs_failures, ["date", "workspaceId", "clusterId", "runId", "jobId"], "inner")\
        .withColumn(
            "revAtRisk",
            col("attributedRevenue")
            * (
                col("revRiskInteractiveJobs")
                + col("revAtRiskUtilization")
                + col("revenuePostReducedFailuresBy50pct") 
            )
        )