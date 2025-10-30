import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

# Load pipeline configuration
load_start_date, change_date = load_confs(spark)
mem_threshold = 50
cpu_threshold = 30


@dlt.view
def workload_insights_view():
    return (
        spark.table("main.data_featurekpi.workload_insights")
        .where(col("date") >= load_start_date)
        .selectExpr(
            "date",
            "clusterId",
            "workloadId",
            "workspaceId",
            "sfdcAccountId",
            "workloadTags.jobId AS jobId",
            "workloadTags.runId AS runId",
            "workloadName",
            "sku",
            "productType",
            """
            CASE 
                WHEN lower(workloadTags.runTerminalState) = 'running' THEN lower(workloadTags.commandStatus)
                WHEN workloadTags.runTerminalState IS NOT NULL THEN lower(workloadTags.runTerminalState)
                WHEN workloadTags.runTerminalState IS NULL AND workloadTags.commandStatus IS NOT NULL THEN lower(workloadTags.commandStatus)
                WHEN workloadTags.runTerminalState IS NULL AND workloadTags.commandStatus IS NULL THEN lower(workloadTags.dltState)
                ELSE 'unknown'
            END AS workloadStatus
            """,
            "attributedRevenue",
            "attributedInfo.attributedDriverDbus AS attributedDriverDbus",
            "attributedInfo.attributedWorkerDbus AS attributedWorkerDbus",
        )
        .where("jobId IS NOT NULL AND runId IS NOT NULL")
    )


@dlt.view
def workspace_customer_mapping_date_filter_view():
    return spark.table(
        "integration.field_mfg_labs.dlt_silver_workspace_customer_mapping"
    ).where(col("date") >= load_start_date)


@dlt.table(name="dlt_silver_workload_insights_jobs")
def silver_workload_insights_jobs():
    jr = spark.read.table("dlt_silver_jobs_utilization").where(
        col("date") >= load_start_date
    )
    wi = spark.read.table("workload_insights_view")
    wcm = spark.read.table("workspace_customer_mapping_date_filter_view")

    result = (
        jr.join(
            wi,
            (jr.date == wi.date)
            & (jr.workspaceId == wi.workspaceId)
            & (jr.clusterId == wi.clusterId)
            & (jr.jobId == wi.jobId)
            & (jr.runId == wi.runId),
            "inner",
        )
        .join(
            wcm,
            (wi.date == wcm.date)
            & (wi.workspaceId == wcm.workspaceId)
            & (wi.sfdcAccountId == wcm.sfdcAccountId),
            "inner",
        )
        .select(
            jr["*"],
            wi.workloadId,
            wi.workloadName,
            wi.sku,
            wi.productType,
            wi.workloadStatus,
            wi.attributedRevenue,
            wi.attributedDriverDbus,
            wi.attributedWorkerDbus,
            wcm.sfdcAccountId,
            wcm.accountId,
            wcm.sfdc_workspace_name,
            wcm.industry_vertical,
            wcm.bu,
            wcm.region,
            wcm.bu_1,
            wcm.bu_2,
            wcm.bu_3,
            wcm.sfdc_account_name,
            wcm.account_name,
            wcm.dbrx_account_owner,
        )
        .withColumn(
            "avg_worker_cpu_utilization",
            col("avg_w_cpu_user_pct") + col("avg_w_cpu_system_pct"),
        )
        .withColumn(
            "avg_driver_cpu_utilization",
            col("avg_d_cpu_user_pct") + col("avg_d_cpu_system_pct"),
        )
        .withColumn(
            "driver_dbu_percentage",
            col("attributedDriverDbus")
            / (col("attributedDriverDbus") + col("attributedWorkerDbus")),
        )
        .withColumn(
            "worker_dbu_percentage",
            col("attributedWorkerDbus")
            / (col("attributedDriverDbus") + col("attributedWorkerDbus")),
        )
        .withColumn(
            "worker_weighted_cpu_utilization",
            col("avg_worker_cpu_utilization") * col("worker_dbu_percentage"),
        )
        .withColumn(
            "driver_weighted_cpu_utilization",
            col("avg_driver_cpu_utilization") * col("driver_dbu_percentage"),
        )
        .withColumn(
            "avg_cluster_cpu_utilization",
            col("avg_worker_cpu_utilization") * col("worker_dbu_percentage")
            + col("avg_driver_cpu_utilization") * col("driver_dbu_percentage"),
        )
        .withColumn(
            "avg_cluster_mem_used_percent",
            col("avg_w_mem_used_pct") * col("worker_dbu_percentage")
            + col("avg_d_mem_used_pct") * col("driver_dbu_percentage"),
        )
        .withColumn(
            "avg_mem_swap_used_percent",
            col("avg_w_mem_swap_pct") * col("worker_dbu_percentage")
            + col("avg_d_mem_swap_pct") * col("driver_dbu_percentage"),
        )
        .withColumn(
            "avg_cpu_wait_percent",
            col("avg_w_cpu_wait_pct") * col("worker_dbu_percentage")
            + col("avg_d_cpu_wait_pct") * col("driver_dbu_percentage"),
        )
        .withColumn(
            "total_network_gb_received",
            (col("sum_d_network_received_bytes") + col("sum_w_network_received_bytes"))
            / lit(1000000000),
        )
        .withColumn(
            "total_network_gb_sent",
            (col("sum_d_network_sent_bytes") + col("sum_w_network_sent_bytes"))
            / lit(1000000000),
        )
        .withColumn(
            "allPurposePenalty",
            when(col("productType") == "JOBS", lit(0))
            # .when(
            #     (col("productType") != "JOBS") & (col("sku").startswith("ENTERPRISE")),
            #     lit(0.692),
            # )
            # .when(
            #     (col("productType") != "JOBS") & (col("sku").startswith("PREMIUM")),
            #     lit(0.727),
            )
            .when((col('productType') != "JOBS"), lit(0.6)) # approximation for now, need to pull in cloud and add cases for azure vs AWS since pricing is different
            .otherwise(lit(None))
            .withColumn(
                "utilizationPenalty",
                (
                    (
                        greatest(
                            lit(cpu_threshold) - col("avg_cluster_cpu_utilization"),
                            lit(0),
                        )
                        / lit(cpu_threshold)
                    )
                    + (
                        greatest(
                            lit(mem_threshold) - col("avg_cluster_mem_used_percent"),
                            lit(0),
                        )
                        / lit(mem_threshold)
                    )
                ),
            )
            .withColumn(
                "revAtRiskUtilization",
                col("attributedRevenue") * col("utilizationPenalty"))
            .withColumn(
                "revAtRiskAllPurpose",
                col("allPurposePenalty")*(col("attributedRevenue")-col("attributedRevenue")*col("utilizationPenalty"))
            .withColumn(
                "revAtRisk",
                col("attributedRevenue")
                * (
                    col("allPurposePenalty")
                    + col("utilizationPenalty")
                    - col("allPurposePenalty") * col("utilizationPenalty")
                ),
            ),
        )
    )
    return result
