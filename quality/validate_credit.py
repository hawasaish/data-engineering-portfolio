from pyspark.sql import functions as F

from spark.spark_session import create_spark_session


INPUT_PATH = (
    "data/gold/customer_risk"
)


def main():

    spark = create_spark_session()

    df = spark.read.parquet(
        INPUT_PATH
    )

    total_records = df.count()

    null_customer_ids = (
        df.filter(
            F.col("customer_id").isNull()
        ).count()
    )

    duplicate_customer_ids = (
        df.groupBy("customer_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    negative_credit_limits = (
        df.filter(
            F.col("credit_limit") < 0
        ).count()
    )

    invalid_risk_levels = (
        df.filter(
            ~F.col("risk_level").isin(
                "LOW",
                "MEDIUM",
                "HIGH"
            )
        ).count()
    )

    invalid_default_flags = (
        df.filter(
            ~F.col("default_flag").isin(
                0,
                1
            )
        ).count()
    )

    negative_utilisation = (
        df.filter(
            F.col("credit_utilisation") < 0
        ).count()
    )

    print("=" * 60)
    print("CREDIT DATA QUALITY REPORT")
    print("=" * 60)

    print(
        f"Total records              : "
        f"{total_records}"
    )

    print(
        f"Null customer IDs          : "
        f"{null_customer_ids}"
    )

    print(
        f"Duplicate customer IDs     : "
        f"{duplicate_customer_ids}"
    )

    print(
        f"Negative credit limits     : "
        f"{negative_credit_limits}"
    )

    print(
        f"Invalid risk levels        : "
        f"{invalid_risk_levels}"
    )

    print(
        f"Invalid default flags      : "
        f"{invalid_default_flags}"
    )

    print(
        f"Negative utilisation       : "
        f"{negative_utilisation}"
    )

    checks = {
        "null_customer_ids": null_customer_ids == 0,
        "duplicate_customer_ids": (
                duplicate_customer_ids == 0
        ),
        "negative_credit_limits": (
                negative_credit_limits == 0
        ),
        "invalid_risk_levels": (
                invalid_risk_levels == 0
        ),
        "invalid_default_flags": (
                invalid_default_flags == 0
        ),
        "negative_utilisation": (
                negative_utilisation == 0
        ),
    }

    failed_checks = [
        name
        for name, passed
        in checks.items()
        if not passed
    ]

    if failed_checks:

        print(
            "FAILED CHECKS:"
        )

        for check in failed_checks:
            print(
                f" - {check}"
            )

        spark.stop()

        raise ValueError(
            "Credit data quality checks failed."
        )

    print(
        "STATUS                    : PASS"
    )

    spark.stop()


if __name__ == "__main__":
    main()