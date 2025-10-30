import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date = load_confs(spark)

@dlt.view
def idle_clusters_metric():
  wiv = spark.read.table('workload_insights_view')
  return wiv.select(
            "date",
            "workspaceId",
            "clusterId",
            when(
                (col("productType") == "ALL_PURPOSE") & (~col("sku").ilike("%serverl%")) & (col("workloadName") == "idleWorkloads"),
                round(col("attributedRevenue"), 2)
            ).otherwise(0).alias("idle_revenue"))