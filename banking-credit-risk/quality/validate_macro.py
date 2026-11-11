from pyspark.sql import functions as F

from spark.spark_session import create_spark_session


INPUT_PATH = (
    "data/silver/macro"
)


def main():

    spark = create_spark_session()

    df = spark.read.parquet(
        INPUT_PATH
    )

    total_records = df.count()

    null_country = (
        df.filter(
            F.col("country_code").isNull()
        ).count()
    )

    null_year = (
        df.filter(
            F.col("year").isNull()
        ).count()
    )

    null_value = (
        df.filter(
            F.col("value").isNull()
        ).count()
    )

    invalid_year = (
        df.filter(
            (F.col("year") < 1900)
            |
            (F.col("year") > 2100)
        ).count()
    )

    expected_indicators = {
        "GDP_GROWTH",
        "INFLATION",
        "UNEMPLOYMENT",
    }

    actual_indicators = {
        row["indicator_name"]
        for row in (
            df.select("indicator_name")
            .distinct()
            .collect()
        )
    }

    missing_indicators = (
            expected_indicators
            - actual_indicators
    )

    print("=" * 60)
    print("MACRO DATA QUALITY REPORT")
    print("=" * 60)

    print(
        f"Total records              : "
        f"{total_records}"
    )

    print(
        f"Null country codes         : "
        f"{null_country}"
    )

    print(
        f"Null years                 : "
        f"{null_year}"
    )

    print(
        f"Null values                : "
        f"{null_value}"
    )

    print(
        f"Invalid years              : "
        f"{invalid_year}"
    )

    print(
        f"Missing indicators         : "
        f"{missing_indicators}"
    )

    if (
            null_country > 0
            or null_year > 0
            or null_value > 0
            or invalid_year > 0
            or missing_indicators
    ):

        spark.stop()

        raise ValueError(
            "Macro data quality checks failed."
        )

    print(
        "STATUS                    : PASS"
    )

    spark.stop()


if __name__ == "__main__":
    main()