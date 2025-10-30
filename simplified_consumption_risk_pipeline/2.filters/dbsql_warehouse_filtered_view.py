import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

load_start_date = load_confs(spark)

@dlt.view
def dbsql_warehouse_filtered_view():
    wfv = spark.read.table('workspace_filter_view')
    return (
        spark.read.table("dbsql_warehouse_view")
        .join(wfv, ["workspaceId"], "left_semi")
    )