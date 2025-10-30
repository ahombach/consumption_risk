import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

# Load pipeline configuration
load_start_date = load_confs(spark)

@dlt.table
def dbsql_metrics():
  df = spark.read.table('dbsql_warehouse_filtered_view')

  return (df.select('date', 
                    'sqlClusterId',
                    'workspaceId',
                    'warehouseType', 
                    'serverlessCompute',
                    'numExecutorsTotal',
                    'numActiveExecutors', 
                    'numActiveTasks', 
                    'numTaskSlots',
                    'numPendingTasks'
                    )
            .groupBy(['date', 'sqlClusterId', 'workspaceId'])
            .agg(
              first('warehouseType').alias('warehouseType'),
              first('serverlessCompute').alias('serverlessCompute'),
              sum('numActiveTasks').alias('numActiveTasks'),
              sum('numTaskSlots').alias('numTaskSlots'),
              avg('numExecutorsTotal').alias('avgNumExecutorsTotal'),
              avg('numActiveExecutors').alias('avgNumActiveExecutors'),
              avg('numPendingTasks').alias('avgNumPendingTasks')
            ))