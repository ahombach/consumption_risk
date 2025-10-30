import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date = load_confs(spark)

@dlt.view
def cluster_sku_view():
  return (spark.read.table("main.centralized_system_tables.billing_usage")
                          .selectExpr("usage_date as date", "account_id as accountId", "workspace_id as workspaceId", "usage_metadata.cluster_id AS clusterId",
                                      "sku_name as skuName", "billing_origin_product as usageOrigin")
                          .where(f"date>= '{load_start_date}'")
                          .where("clusterId is not null")
                          .distinct()
                      )
  