import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

# Load pipeline configuration
load_start_date, change_date = load_confs(spark)

@dlt.view
def dlt_dbsql_metrics():
  df = spark.read.table('main.eng_dbsql.cluster_metrics_stream').where('date' > lit(load_start_date))

  return (df
            .select('date', 
                    'workspaceId', 
                    'serverlessCompute', 
                    'numActiveTasks', 
                    'numTaskSlots')
            .groupBy(['date', 'workspaceId', 'serverlessCompute'])
            .agg(
              sum('numActiveTasks').alias('numActiveTasks'),
              sum('numTaskSlots').alias('numTaskSlots')
            ))