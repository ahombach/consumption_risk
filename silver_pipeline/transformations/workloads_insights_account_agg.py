import dlt
from pyspark.sql.functions import *
from utils.confloader import load_confs

# Load pipeline configuration
load_start_date, change_date = load_confs(spark)

@dlt.view
def dlt_workload_insights_core_view():
    wi = spark.read.table("workload_insights_view")   
    wlv = spark.read.table("workspace_customer_mapping_view")

    joined = (
        wi.join(
            wlv, 
            ['date', 'workspaceId', 'sfdcAccountId'],
            "inner"
        )
        .filter(wi["date"] >= lit(load_start_date))
    )

    result = (
        joined.select(
            "date", "workspaceId", "sfdcAccountId", "industry_vertical", "bu", "region", "bu_1", "bu_2", "bu_3",
            "sfdc_account_name", "account_name", "sfdc_workspace_name", "dbrx_account_owner",
            when(
                (col("productType") == "ALL_PURPOSE") & (~col("sku").ilike("%serverl%")) & (col("workloadName") == "idleWorkloads"),
                round(col("attributedRevenue"), 2)
            ).otherwise(0).alias("idle_revenue"),
            when(
                (col("productType") == "ALL_PURPOSE") & (~col("sku").ilike("%serverl%")) & (col("workloadName") == "jobRuns"),
                when(
                    col("sku").isin(
                        "ENTERPRISE_ALL_PURPOSE_COMPUTE", "ENTERPRISE_ALL_PURPOSE_COMPUTE_(PHOTON)",
                        "ENTERPRISE_ALL_PURPOSE_COMPUTE_GOV", "ENTERPRISE_ALL_PURPOSE_COMPUTE_(PHOTON)_GOV"
                    ),
                    round(col("attributedRevenue") * (1 - 0.3), 2)
                ).when(
                    col("sku").isin(
                        "STANDARD_ALL_PURPOSE_COMPUTE", "STANDARD_ALL_PURPOSE_COMPUTE_(PHOTON)"
                    ),
                    round(col("attributedRevenue") * (1 - 0.375), 2)
                ).when(
                    col("sku").isin(
                        "PREMIUM_ALL_PURPOSE_COMPUTE", "PREMIUM_ALL_PURPOSE_COMPUTE_(PHOTON)"
                    ),
                    round(col("attributedRevenue") * (1 - 0.545), 2)
                ).otherwise(0)
            ).otherwise(0).alias("all_purpose_job_revenue"),
            when(
                (col("productType") == "ALL_PURPOSE") & (~col("sku").ilike("%serverl%")) & (col("workloadName") == "jobRuns"),
                when(
                    col("sku").isin(
                        "ENTERPRISE_ALL_PURPOSE_COMPUTE", "ENTERPRISE_ALL_PURPOSE_COMPUTE_(PHOTON)",
                        "ENTERPRISE_ALL_PURPOSE_COMPUTE_GOV", "ENTERPRISE_ALL_PURPOSE_COMPUTE_(PHOTON)_GOV"
                    ),
                    round(col("attributedRevenue") * 0.3, 2)
                ).when(
                    col("sku").isin(
                        "STANDARD_ALL_PURPOSE_COMPUTE", "STANDARD_ALL_PURPOSE_COMPUTE_(PHOTON)"
                    ),
                    round(col("attributedRevenue") * 0.375, 2)
                ).when(
                    col("sku").isin(
                        "PREMIUM_ALL_PURPOSE_COMPUTE", "PREMIUM_ALL_PURPOSE_COMPUTE_(PHOTON)"
                    ),
                    round(col("attributedRevenue") * 0.545, 2)
                ).otherwise(round(col("attributedRevenue"), 2))
            ).otherwise(round(col("attributedRevenue"), 2)).alias("corrected_all_purpose_job_sku_revenue"),
            round(col("attributedRevenue"), 2).alias("rounded_attributedRevenue"),
            col("sku").ilike("%SQL%").cast("boolean").alias("wi_sqlCompute"),
            (col("sku").ilike("%SQL%") & col("sku").ilike("%SERVERLESS%")).cast("boolean").alias("wi_serverlessCompute"),
            when(
                col("workloadStatus").isin("cancelled", "error", "failed", "skipped", "timedout", "canceled"),
                "failed"
            ).otherwise("success").alias("workloadStatus"),
            when(
                col("workloadStatus").isin("cancelled", "error", "failed", "skipped", "timedout", "canceled"),
                round(col("attributedRevenue") / 2, 2)
            ).otherwise(0).alias("failedWorkloadRevenue50pct"),
            when(
                col("workloadStatus").isin("cancelled", "error", "failed", "skipped", "timedout", "canceled"),
                round(col("attributedRevenue") / 2, 2)
            ).otherwise(round(col("attributedRevenue"), 2)).alias("revenuePostReducedFailuresBy50pct")
        )
    )
    return result
  
@dlt.view
def wi_silver():
    wi_core = spark.read.table("dlt_workload_insights_core_view")
    return (
        wi_core.groupBy(
            "date", "industry_vertical", "bu", "region", "bu_1", "bu_2", "bu_3",
            "sfdc_account_name", "account_name", "sfdc_workspace_name", "dbrx_account_owner"
        ).agg(
            sum("idle_revenue").alias("idle_revenue"),
            sum("all_purpose_job_revenue").alias("interactive_risk_dollars"),
            sum("corrected_all_purpose_job_sku_revenue").alias("interactive_usage_dollar_adjusted_100pct"),
            sum("rounded_attributedRevenue").alias("usage_amount"),
            sum("failedWorkloadRevenue50pct").alias("failed_workload_risk_dollars"),
            sum("revenuePostReducedFailuresBy50pct").alias("failed_workload_usage_dollar_adjusted_50pct")
        )
    )

@dlt.view
def dbsql_bronze_bu1():
    wi_core = spark.read.table("dlt_workload_insights_core_view")
    dbsql_core = spark.read.table("dlt_dbsql_metrics")
    wi = wi_core.filter("wi_sqlCompute = true")
    joined = wi.join(
        dbsql_core,
        (wi["date"] == dbsql_core["date"]) &
        (wi["workspaceId"] == dbsql_core["workspaceId"]) &
        (wi["wi_serverlessCompute"] == dbsql_core["serverlessCompute"]),
        "left"
    ).drop(dbsql_core.date)\
     .drop(dbsql_core.workspaceId)\
     .drop(dbsql_core.serverlessCompute)   
    return (
        joined.groupBy("date", "bu_1")
        .agg(
            sum("numActiveTasks").alias("dat_bu1"),
            sum("numTaskSlots").alias("dts_bu1")
        )
    )

@dlt.view
def dbsql_bronze_acnt():
    wi_core = spark.read.table("dlt_workload_insights_core_view")
    dbsql_core = spark.read.table("dlt_dbsql_metrics")
    dbsql_bu1 = spark.read.table("dbsql_bronze_bu1")
    wi = wi_core.filter("wi_sqlCompute = true")
    joined = (
        wi.join(
            dbsql_core,
            (wi["date"] == dbsql_core["date"]) &
            (wi["workspaceId"] == dbsql_core["workspaceId"]) &
            (wi["wi_serverlessCompute"] == dbsql_core["serverlessCompute"]),
            "left"
        ).drop(dbsql_core.date)\
         .drop(dbsql_core.workspaceId)\
         .drop(dbsql_core.serverlessCompute)\
        .join(
            dbsql_bu1,
            ["date", "bu_1"],
            "left"
        )
    )
    return (
        joined.groupBy(
            "date", "industry_vertical", "bu", "region", "bu_1", "bu_2", "bu_3",
            "sfdc_account_name", "account_name", "sfdc_workspace_name", "dbrx_account_owner",
            "dat_bu1", "dts_bu1"
        ).agg(
            sum("numActiveTasks").alias("dat_acnt"),
            sum("numTaskSlots").alias("dts_acnt"),
            sum("rounded_attributedRevenue").alias("sql_dollars")
        )
    )

@dlt.view
def wi_join_dsql_silver():
    wi_silver = spark.read.table("wi_silver")
    dbsql_bronze_acnt = spark.read.table("dbsql_bronze_acnt")
    joined = wi_silver.join(
        dbsql_bronze_acnt,
        ["date", "industry_vertical", "bu", "region", "bu_1", "bu_2", "bu_3","sfdc_account_name", "account_name", "sfdc_workspace_name", "dbrx_account_owner"],
        "left"
    )
    return (
        joined.groupBy(
            "date", "industry_vertical", "bu", "region", "bu_1", "bu_2", "bu_3",
            "sfdc_account_name", "account_name", "sfdc_workspace_name", "dbrx_account_owner"
        ).agg(
            sum("idle_revenue").alias("idle_revenue"),
            sum("interactive_risk_dollars").alias("interactive_risk_dollars"),
            sum("interactive_usage_dollar_adjusted_100pct").alias("interactive_usage_dollar_adjusted_100pct"),
            sum("usage_amount").alias("usage_amount"),
            sum("dat_bu1").alias("dat_bu1"),
            sum("dts_bu1").alias("dts_bu1"),
            sum("dat_acnt").alias("dat_acnt"),
            sum("dts_acnt").alias("dts_acnt"),
            sum("sql_dollars").alias("sql_dollars"),
            sum("failed_workload_risk_dollars").alias("failed_workload_risk_dollars")
        )
    )


@dlt.view
def avg_all_levels():
    df = spark.read.table("wi_join_dsql_silver")
    return (
        df.select(
            "date",
            "industry_vertical", "bu", "region",
            "bu_1", "bu_2", "bu_3", "sfdc_account_name", "account_name", "sfdc_workspace_name", "dbrx_account_owner",
            (col("dat_bu1") / col("dts_bu1")).alias("overall_avg_utilization_bu1"),
            (col("dat_acnt") / col("dts_acnt")).alias("overall_avg_utilization_acnt"),
            "sql_dollars",
            "usage_amount",
            "idle_revenue",
            "interactive_risk_dollars",
            "interactive_usage_dollar_adjusted_100pct",
            "failed_workload_risk_dollars"
        )
    )

@dlt.view
def risk_all_levels():
    df = spark.read.table("avg_all_levels")
    return (
        df.select(
            "date",
            "industry_vertical", "bu", "region",
            "bu_1", "bu_2", "bu_3", "sfdc_account_name", "account_name", "sfdc_workspace_name", "dbrx_account_owner",
            "overall_avg_utilization_bu1",
            "overall_avg_utilization_acnt",
            "sql_dollars",
            when(
                col("overall_avg_utilization_bu1") >= col("overall_avg_utilization_acnt"),
                (col("overall_avg_utilization_bu1") - col("overall_avg_utilization_acnt")) / col("overall_avg_utilization_bu1")
            ).otherwise(0).alias("risk_pct_bu1"),
            when(
                col("overall_avg_utilization_bu1") >= col("overall_avg_utilization_acnt"),
                ((col("overall_avg_utilization_bu1") - col("overall_avg_utilization_acnt")) / col("overall_avg_utilization_bu1")) * col("sql_dollars")
            ).otherwise(0).alias("at_risk_dollars_bu1"),
            when(
                col("overall_avg_utilization_bu1") >= col("overall_avg_utilization_acnt"),
                (col("sql_dollars") * col("overall_avg_utilization_bu1")) * ((col("overall_avg_utilization_bu1") - col("overall_avg_utilization_acnt")) / col("overall_avg_utilization_bu1"))
            ).otherwise(0).alias("at_risk_dollars_bu1_revised"),
            "usage_amount",
            "idle_revenue",
            "interactive_risk_dollars",
            "interactive_usage_dollar_adjusted_100pct",
            "failed_workload_risk_dollars"
        )
    )

@dlt.table(
    comment="Materialized view aggregating risk and usage dollars"
)
def risk_and_usage_mv():
    df = spark.read.table("risk_all_levels")
    return (
        df.select(
            "*",
            (col("interactive_risk_dollars") + col("idle_revenue") + col("at_risk_dollars_bu1_revised") + col("failed_workload_risk_dollars")).alias("risk_dollars"),
            (col("interactive_usage_dollar_adjusted_100pct") - (col("idle_revenue") + col("at_risk_dollars_bu1_revised") + col("failed_workload_risk_dollars"))).alias("usage_dollar_adjusted_100pct")
        )
    )