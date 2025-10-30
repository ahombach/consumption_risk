import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

# Load pipeline configuration
load_start_date = load_confs(spark)

@dlt.view
def dbsql_warehouse_view():
  df = spark.read.table('main.eng_dbsql.cluster_metrics_stream').where('date' > lit(load_start_date))

  return (df.select('date', 
                    'timestamp',
                    'sqlClusterId',
                    'workspaceId',
                    'warehouseType', 
                    'serverlessCompute', 
                    'numExecutorsTotal',
                    'numActiveExecutors',
                    'numActiveTasks', 
                    'numTaskSlots',
                    'numPendingTasks',
                    'isAtFullCapacity')
            )