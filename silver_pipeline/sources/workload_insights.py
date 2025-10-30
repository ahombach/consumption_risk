import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date, change_date = load_confs(spark)

@dlt.view
def workload_insights_view():
    return (
        spark.table("main.data_featurekpi.workload_insights")
        .where(col("date") >= load_start_date)
        .select(
            col("date"),
            col("clusterId"),
            col("workloadId"),
            col("workspaceId"),
            col("sfdcAccountId"),
            col("workloadTags.jobId").alias("jobId"),
            col("workloadTags.runId").alias("runId"),
            col("workloadName"),
            col("sku"),
            col("productType"),
            when(lower(col("workloadTags.runTerminalState")) == "running", lower(col("workloadTags.commandStatus")))
            .when(col("workloadTags.runTerminalState").isNotNull(), lower(col("workloadTags.runTerminalState")))
            .when(col("workloadTags.runTerminalState").isNull() & col("workloadTags.commandStatus").isNotNull(), lower(col("workloadTags.commandStatus")))
            .when(col("workloadTags.runTerminalState").isNull() & col("workloadTags.commandStatus").isNull(), lower(col("workloadTags.dltState")))
            .otherwise("unknown")
            .alias("workloadStatus"),
            col("attributedRevenue"),
            col("attributedInfo.attributedDriverDbus").alias("attributedDriverDbus"),
            col("attributedInfo.attributedWorkerDbus").alias("attributedWorkerDbus"),
        )
      )

