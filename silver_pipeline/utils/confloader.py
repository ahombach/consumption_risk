import dlt 

def load_confs(spark):
    load_start_date = str(spark.conf.get("jobs_utilization.start_date"))
    change_date = str(spark.conf.get("compute_clusters.change_date"))
    return load_start_date, change_date