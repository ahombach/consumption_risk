import dlt
from pyspark.sql.functions import *

@dlt.view
def workspaces_latest_view():
  return (spark.table("main.certified.workspaces_latest")
                             .where("""lower(trim(workspace_status)) = 'running' and lower(trim(inferred_workspace_type)) = 'external'""")
                             .where("""salesforce_account_name is not null and salesforce_account_id is not null""")
                             .where("""lower(trim(salesforce_account_name)) not in ('databricks','microsoft','databricks labs','vocareum, inc.','generic/public business subscription account')""")
                             .where("customer_id is not null")
                             .where("customer_id not in (select distinct sfdc_account_id from main.fin_live_gold.live_pipeline_excluded_accounts)")
                             .selectExpr("workspace_id as workspaceId", "customer_id as sfdcAccountId", "account_id as accountId")
                        )
