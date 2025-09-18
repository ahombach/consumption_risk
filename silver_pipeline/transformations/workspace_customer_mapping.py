import dlt
from pyspark.sql.functions import *

@dlt.view
def workspace_customer_mapping_view():
  return (spark.readStream.table("main.fin_live_gold.paid_usage_metering")
                                             .where("date >= '2024-02-01'")
                                            #  .where(f"date < date_sub(current_date(), 2)")
                                             .selectExpr("date", "sfdc_account_id as sfdcAccountId", "platform_account_id as accountId", "sfdc_workspace_id as workspaceId",
                                                         "sfdc_workspace_name", "industry_vertical", "bu", "region", "sfdc_region_l1 as bu_1", "sfdc_region_l2 as bu_2",
                                                         "sfdc_region_l3 as bu_3", "sfdc_account_name", "deployable_account_name as account_name",
                                                         "sfdc_owner_name as dbrx_account_owner")
                                             )
  
  
@dlt.table(
  name="dlt_silver_workspace_customer_mapping",
  cluster_by = ["date", "workspaceId", "sfdcAccountId", "accountId"]
)
def workspace_customer_mapping():

  wlv = spark.read.table('workspaces_latest_view')

  return (spark.readStream.table("workspace_customer_mapping_view")
          .join(wlv, ["workspaceId", "sfdcAccountId", "accountId"], "left_semi"))