import dlt
from pyspark.sql.functions import *
  
@dlt.table(
  name="dlt_silver_workspace_customer_mapping",
  cluster_by = ["date", "workspaceId", "sfdcAccountId", "accountId"]
)
def workspace_customer_mapping():

  wlv = spark.read.table('workspaces_latest_view')

  return (spark.read.table("workspace_customer_mapping_view")
          .join(wlv, ["workspaceId", "sfdcAccountId", "accountId"], "left_semi"))