import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date, change_date = load_confs(spark)

@dlt.table(
  name="dlt_silver_compute_clusters",
)
def compute_clusters_ww():

  wlv = spark.read.table('workspaces_latest_view')

  return (spark.read.table("compute_clusters_view")
          .join(wlv, ["workspaceId", "accountId"], "left_semi"))


  

  
