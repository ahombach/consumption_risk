import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date = load_confs(spark)

@dlt.view
def job_runs_interactive():
    wiv = spark.read.table("workload_insights_view")\
            .where(col("runId").isNotNull())\
            .select('date', 
                    'workspaceId', 
                    'clusterId', 
                    'jobId', 
                    'runId',
                    'productType',
                    'attributedRevenue')

    return wiv\
        .withColumn(
            "allPurposePenalty",
            when(col("productType") != "JOBS", lit(0.6))
            .otherwise(lit(0)))\
        .withColumn(
            "revRiskInteractiveJobs",
            col("allPurposePenalty")*(col("attributedRevenue")))