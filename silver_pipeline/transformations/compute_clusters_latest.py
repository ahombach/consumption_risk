import dlt
from pyspark.sql.functions import *

@dlt.view
def compute_clusters_view():
  return (spark.readStream.table("main.centralized_system_tables.compute_clusters")
                                             .where("change_date>= '2024-02-01'")
                                             .selectExpr("cluster_id as clusterId", 
                                                         "workspace_id as workspaceId",
                                                         "account_id as accountId",
                                                         "auto_termination_minutes as autoTerminationMinutes",
                                                         "change_time as changeTime"
                                                         )
                                             )
  
@dlt.view(
  name="compute_clusters_latest_view",
)
def compute_clusters_latest_view():

  wlv = spark.read.table('workspaces_latest_view')

  return (spark.readStream.table("compute_clusters_view")
          .join(wlv, ["workspaceId", "accountId"], "left_semi"))

dlt.create_streaming_table(
  name = "dlt_silver_compute_clusters_latest",
  cluster_by = ["clusterId", "workspaceId", "accountId"],
)

dlt.create_auto_cdc_flow(
  target = "dlt_silver_compute_clusters_latest",
  source = "compute_clusters_latest_view",
  keys = ['clusterId','workspaceId', 'accountId'],
  sequence_by = col("changeTime"),
  stored_as_scd_type = 1
)
  

  
