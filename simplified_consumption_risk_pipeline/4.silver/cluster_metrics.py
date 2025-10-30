import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from utils.confloader import load_confs

load_start_date = load_confs(spark)

@dlt.table
def cluster_metrics():
  # Idle revenue by day & cluster
  cit = spark.read.table("idle_clusters_metric")\
        .withColumnRenamed('clusterId', 'cit_clusterId')\
        .withColumnRenamed('workspaceId', 'cit_workspaceId')

  # Cluster configuration history (one row per change)
  ccv = spark.read.table("compute_clusters_view")

  # Derive the end date for each configuration period so we can perform a range join
  w = Window.partitionBy("workspaceId", "clusterId").orderBy(col("changeDate"))
  ccv_periods = (
      ccv.withColumn("endDate", lead("changeDate").over(w))
         .withColumn("endDate", date_sub(col("endDate"), 1))  # last day the config is valid (inclusive)
  )

  join_cond = (
      (col("cit_workspaceId") == col("workspaceId")) &
      (col("cit_clusterId") == col("clusterId")) &
      (col("date") >= col("changeDate")) &
      ((col("date") <= col("endDate")) | col("endDate").isNull())
  )

  return cit.join(ccv_periods, join_cond, "left").drop('cit_clusterId', 'cit_workspaceId')