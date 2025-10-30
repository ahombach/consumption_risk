import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date, change_date = load_confs(spark)

@dlt.view
def workspace_customer_mapping_view():
  return (spark.read.table("main.fin_live_gold.paid_usage_metering")
                                             .where(col('date') >= lit(load_start_date))
                                            #  .where(f"date < date_sub(current_date(), 2)")
                                             .selectExpr("date", 
                                                         "sfdc_account_id as sfdcAccountId", 
                                                         "platform_account_id as accountId", 
                                                         "sfdc_workspace_id as workspaceId",
                                                         "sfdc_workspace_name", 
                                                         "industry_vertical", 
                                                         "bu", 
                                                         "region", 
                                                         "sfdc_region_l1 as bu_1", 
                                                         "sfdc_region_l2 as bu_2",
                                                         "sfdc_region_l3 as bu_3", 
                                                         "sfdc_account_name", 
                                                         "sfdc_account_id",
                                                         "deployable_account_name as account_name",
                                                         "sfdc_owner_name as dbrx_account_owner")
                                             .distinct()
                                             )
  
