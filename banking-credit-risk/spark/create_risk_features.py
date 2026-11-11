from pyspark.sql import functions as F

from spark.spark_session import create_spark_session


INPUT_PATH = (
    "data/silver/credit"
)

OUTPUT_PATH = (
    "data/gold/customer_risk"
)


def main():

    spark = create_spark_session()

    df = spark.read.parquet(
        INPUT_PATH
    )

    # --------------------------------------------------
    # Repayment status columns
    # --------------------------------------------------

    payment_status_columns = [
        "pay_0",
        "pay_2",
        "pay_3",
        "pay_4",
        "pay_5",
        "pay_6",
    ]

    # --------------------------------------------------
    # Delinquency count
    #
    # Positive repayment status indicates delay.
    # --------------------------------------------------

    delinquency_expression = sum(
        F.when(
            F.col(column) > 0,
            1
        ).otherwise(0)
        for column in payment_status_columns
    )

    df = df.withColumn(
        "delinquency_count",
        delinquency_expression
    )

    # --------------------------------------------------
    # Average payment status
    # --------------------------------------------------

    df = df.withColumn(
        "avg_payment_status",
        sum(
            F.coalesce(
                F.col(column),
                F.lit(0)
            )
            for column in payment_status_columns
        ) / len(payment_status_columns)
    )

    # --------------------------------------------------
    # High utilisation flag
    # --------------------------------------------------

    df = df.withColumn(
        "high_utilisation_flag",
        F.when(
            F.col("credit_utilisation") >= 0.80,
            1
        ).otherwise(0)
    )

    # --------------------------------------------------
    # Low payment ratio flag
    # --------------------------------------------------

    df = df.withColumn(
        "low_payment_ratio_flag",
        F.when(
            F.col("payment_ratio") < 0.30,
            1
        ).otherwise(0)
    )

    # --------------------------------------------------
    # Risk score
    # --------------------------------------------------

    df = df.withColumn(
        "risk_score",

        (
                F.when(
                    F.col("credit_utilisation") >= 0.80,
                    40
                ).otherwise(0)

                +

                F.when(
                    (
                            F.col("credit_utilisation") >= 0.50
                    )
                    &
                    (
                            F.col("credit_utilisation") < 0.80
                    ),
                    20
                ).otherwise(0)

                +

                F.when(
                    F.col("delinquency_count") >= 3,
                    30
                )
                .when(
                    F.col("delinquency_count") == 2,
                    20
                )
                .when(
                    F.col("delinquency_count") == 1,
                    10
                )
                .otherwise(0)

                +

                F.when(
                    F.col("payment_ratio") < 0.30,
                    20
                ).otherwise(0)
        )
    )

    # --------------------------------------------------
    # Risk level
    # --------------------------------------------------

    df = df.withColumn(
        "risk_level",

        F.when(
            F.col("risk_score") >= 60,
            "HIGH"
        )
        .when(
            F.col("risk_score") >= 30,
            "MEDIUM"
        )
        .otherwise(
            "LOW"
        )
    )

    # --------------------------------------------------
    # Default label
    # --------------------------------------------------

    df = df.withColumn(
        "default_flag",
        F.col("default_flag").cast("integer")
    )

    # --------------------------------------------------
    # Select Gold columns
    # --------------------------------------------------

    gold_df = df.select(
        "customer_id",

        "credit_limit",

        "gender",
        "education_level",
        "marital_status",
        "age",
        "age_group",

        "total_bill_amount",
        "total_payment_amount",

        "avg_bill_amount",
        "avg_payment_amount",

        "credit_utilisation",
        "payment_ratio",

        "delinquency_count",
        "avg_payment_status",

        "high_utilisation_flag",
        "low_payment_ratio_flag",

        "risk_score",
        "risk_level",

        "default_flag",
    )

    # --------------------------------------------------
    # Write Gold
    # --------------------------------------------------

    (
        gold_df.write
        .mode("overwrite")
        .partitionBy(
            "risk_level"
        )
        .parquet(
            OUTPUT_PATH
        )
    )

    print(
        f"Gold customer records: "
        f"{gold_df.count()}"
    )

    gold_df.groupBy(
        "risk_level"
    ).count().show()

    spark.stop()


if __name__ == "__main__":
    main()