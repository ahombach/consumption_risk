import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date, change_date = load_confs(spark)

@dlt.table
def dlt_workloads_cluster_agg():
  df = (spark.read.table('workspace_customer_mapping_view')
        .select('workspaceId', "bu", "bu_1", "bu_2", "bu_3", "sfdc_workspace_name", "sfdc_account_name"))

  return (spark.read.table('main.data_df_metering.workloads_cluster_agg')
          .select('date', 'sku', 'workloadType', 'accountId', 'workspaceId', 'clusterId', 'memoryPerNodeInMb', 'numCpusPerNode', 'dbus')
          .where(col('date') >= lit(load_start_date))
          .groupBy(['date', 'sku', 'workloadType', 'workspaceId'])
          .agg(
            count('clusterId').alias('cluster_id_count'),
            avg('memoryPerNodeInMb').alias('avg_memory_per_node_mb'),
            avg('numCpusPerNode').alias('avg_num_cpus_per_node'),
            sum('dbus').alias('sum_dbus')
          ).join(df, on=['workspaceId'], how="left"))




