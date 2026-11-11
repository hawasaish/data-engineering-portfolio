from pyspark.sql import functions as F

from spark.spark_session import create_spark_session


CUSTOMER_RISK_PATH = (
    "data/gold/customer_risk"
)

MACRO_PATH = (
    "data/silver/macro"
)

PORTFOLIO_PATH = (
    "data/gold/portfolio_risk"
)

MACRO_SNAPSHOT_PATH = (
    "data/gold/macro_risk"
)


def main():

    spark = create_spark_session()

    # --------------------------------------------------
    # Customer risk
    # --------------------------------------------------

    customer_df = spark.read.parquet(
        CUSTOMER_RISK_PATH
    )

    # --------------------------------------------------
    # Portfolio summary
    # --------------------------------------------------

    portfolio_df = (
        customer_df
        .groupBy("risk_level")
        .agg(
            F.count("*").alias(
                "customer_count"
            ),

            F.sum(
                "credit_limit"
            ).alias(
                "total_credit_limit"
            ),

            F.avg(
                "credit_utilisation"
            ).alias(
                "avg_credit_utilisation"
            ),

            F.avg(
                "payment_ratio"
            ).alias(
                "avg_payment_ratio"
            ),

            F.avg(
                "delinquency_count"
            ).alias(
                "avg_delinquency_count"
            ),

            F.avg(
                "default_flag"
            ).alias(
                "default_rate"
            )
        )
    )

    portfolio_df = portfolio_df.withColumn(
        "default_rate_percent",
        F.col("default_rate") * 100
    )

    (
        portfolio_df.write
        .mode("overwrite")
        .parquet(
            PORTFOLIO_PATH
        )
    )

    # --------------------------------------------------
    # Macro latest snapshot
    # --------------------------------------------------

    macro_df = spark.read.parquet(
        MACRO_PATH
    )

    latest_year = (
        macro_df
        .agg(
            F.max("year")
            .alias("latest_year")
        )
        .collect()[0]["latest_year"]
    )

    macro_latest = macro_df.filter(
        F.col("year") == latest_year
    )

    macro_snapshot = (
        macro_latest
        .groupBy(
            "country_code",
            "country_name",
            "year"
        )
        .pivot(
            "indicator_name"
        )
        .agg(
            F.first("value")
        )
    )

    (
        macro_snapshot.write
        .mode("overwrite")
        .parquet(
            MACRO_SNAPSHOT_PATH
        )
    )

    print(
        f"Latest macro year: {latest_year}"
    )

    print(
        "Portfolio summary:"
    )

    portfolio_df.show(
        truncate=False
    )

    print(
        "Macro snapshot:"
    )

    macro_snapshot.show(
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()