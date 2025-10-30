import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date, change_date = load_confs(spark)

@dlt.view
def compute_clusters_view():
  return (spark.read.table("main.centralized_system_tables.compute_clusters")
                                             .where(col("change_date") >= lit(change_date))
                                             .selectExpr("cluster_id as clusterId", 
                                                         "workspace_id as workspaceId",
                                                         "account_id as accountId",
                                                         "auto_termination_minutes as autoTerminationMinutes",
                                                         "change_time as changeTime"
                                                         )
                                             )
