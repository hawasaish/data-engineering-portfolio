from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def test_risk_classification():

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName(
            "BankingRiskTest"
        )
        .getOrCreate()
    )

    data = [
        (
            1,
            0.90,
            4,
            0.20
        ),

        (
            2,
            0.40,
            1,
            0.80
        ),

        (
            3,
            0.20,
            0,
            1.00
        ),
    ]

    df = spark.createDataFrame(
        data,

        [
            "customer_id",
            "credit_utilisation",
            "delinquency_count",
            "payment_ratio",
        ]
    )

    df = df.withColumn(
        "risk_level",

        F.when(
            (
                    F.col(
                        "credit_utilisation"
                    ) >= 0.80
            )
            |
            (
                    F.col(
                        "delinquency_count"
                    ) >= 3
            ),

            "HIGH"
        )
        .otherwise(
            "LOW"
        )
    )

    results = {
        row["customer_id"]:
            row["risk_level"]

        for row in df.collect()
    }

    assert results[1] == "HIGH"

    assert results[3] == "LOW"

    spark.stop()