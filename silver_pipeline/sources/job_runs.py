import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date, change_date = load_confs(spark)

@dlt.view
def job_runs_view():
  return (spark.read.table("main.data_usage_log_summary.job_runs")
               .where(col("date") >= lit(load_start_date))
               .selectExpr("date", "workspaceId", "clusterId", "jobId", "runId",
                           "timestampadd(second, -durationSeconds, runTerminationTime) AS runStartTime",
                           "date_trunc('minute', timestampadd(second, -durationSeconds, runTerminationTime)) AS runStartTime_floor",
                           "date_add(MINUTE, 1, date_trunc('minute', runTerminationTime)) AS runTerminationTime_ceil",
                           "durationSeconds", "runTerminationTime", "runTerminalState", "runClusterType", "runTaskType", "runTriggerType"
               )
               )