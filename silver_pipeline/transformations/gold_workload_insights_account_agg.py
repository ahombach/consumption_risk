import dlt
from pyspark.sql.functions import *

@dlt.table
def dlt_gold_workload_insights_acct_agg():
  df = spark.read.table("dlt_silver_workload_insights_jobs")
  
  return (df.select('accountId', 
                    'sfdc_account_name', 
                    'bu', 
                    'region', 
                    'bu_1', 
                    'bu_2', 
                    'bu_3', 
                    'date',
                    'durationSeconds',
                    'avg_cluster_cpu_utilization',
                    'avg_cluster_mem_used_percent',
                    'total_network_gb_sent',
                    'total_network_gb_received',
                    'runId',
                    'attributedRevenue',
                    'revAtRiskUtilization',
                    'revAtRiskAllPurpose',
                    'revAtRisk')
            .groupBy(["accountId", "sfdc_account_name", "bu", "region", "bu_1", "bu_2", "bu_3", "date"])
            .agg(
              sum("durationSeconds").alias("total_duration_seconds"),
              avg("avg_cluster_cpu_utilization").alias('avg_cluster_cpu_utilization'),
              avg("avg_cluster_mem_used_percent").alias('avg_cluster_mem_used_percent'),
              avg("total_network_gb_sent").alias('avg_cluster_network_gb_sent'),
              avg("total_network_gb_received").alias('avg_cluster_network_gb_received'),
              countDistinct("runId").alias('job_run_count'),
              sum('attributedRevenue').alias("dollarDbus"),
              sum('revAtRisk').alias('total_revenue_at_risk'),
              sum('revAtRiskUtilization').alias('revenue_at_risk_utilization'),  
              sum('revAtRiskAllPurpose').alias('revenue_at_risk_all_purpose_jobs')             
          ))
  

