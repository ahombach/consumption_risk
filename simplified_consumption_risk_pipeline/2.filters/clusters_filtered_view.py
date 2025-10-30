import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date = load_confs(spark)

@dlt.view
def compute_clusters_filtered_view():

  wfv = spark.read.table('workspace_filter_view')

  return (spark.read.table("compute_clusters_view")
          .join(wfv, ["workspaceId", "accountId"], "left_semi"))
