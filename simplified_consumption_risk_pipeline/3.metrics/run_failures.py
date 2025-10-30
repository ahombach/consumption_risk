import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date = load_confs(spark)
        
@dlt.view
def job_runs_failures():
  wiv = spark.read.table("workload_insights_view")\
          .where(col("runId").isNotNull())\
          .select("date", 
                  "workspaceId", 
                  "clusterId", 
                  "jobId", 
                  "runId", 
                  "workloadStatus", 
                  "attributedRevenue")

  return wiv\
        .withColumn(
            "jobStatus",
            when(
                col("workloadStatus").isin("cancelled", "error", "failed", "skipped", "timedout", "canceled"),
                "failed"
            ).otherwise("success"))\
        .withColumn(
            "failedWorkloadRevenue50pct",
            when(
                col("workloadStatus").isin("cancelled", "error", "failed", "skipped", "timedout", "canceled"),
                round(col("attributedRevenue") / 2, 2)
            ).otherwise(0))\
        .withColumn(
            "revenuePostReducedFailuresBy50pct",
            when(
                col("workloadStatus").isin("cancelled", "error", "failed", "skipped", "timedout", "canceled"),
                round(col("attributedRevenue") / 2, 2)
            ).otherwise(round(col("attributedRevenue"), 2)))