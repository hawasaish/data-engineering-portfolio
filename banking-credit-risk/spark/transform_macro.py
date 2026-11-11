from pyspark.sql import functions as F

from spark.spark_session import create_spark_session


GDP_PATH = (
    "s3a://banking-risk/bronze/macro_api/gdp_growth/"
)

INFLATION_PATH = (
    "s3a://banking-risk/bronze/macro_api/inflation/"
)

UNEMPLOYMENT_PATH = (
    "s3a://banking-risk/bronze/macro_api/unemployment/"
)

SILVER_PATH = (
    "data/silver/macro"
)


def transform_indicator(
        df,
        indicator_name
):

    return (
        df
        .select(
            F.lit(indicator_name)
            .alias("indicator_name"),

            F.col("indicator_code"),

            F.explode(
                F.col("data")
            ).alias("record")
        )
        .select(
            "indicator_name",
            "indicator_code",

            F.col(
                "record.countryiso3code"
            ).alias("country_code"),

            F.col(
                "record.country.value"
            ).alias("country_name"),

            F.col(
                "record.date"
            )
            .cast("integer")
            .alias("year"),

            F.col(
                "record.value"
            )
            .cast("double")
            .alias("value")
        )
        .filter(
            F.col("value").isNotNull()
        )
    )


def main():

    spark = create_spark_session()

    # --------------------------------------------------
    # Read GDP
    # --------------------------------------------------

    gdp_df = (
        spark.read
        .option("multiLine", True)
        .json(GDP_PATH)
    )

    print("GDP raw schema:")
    gdp_df.printSchema()

    gdp_clean = transform_indicator(
        gdp_df,
        "GDP_GROWTH"
    )

    # --------------------------------------------------
    # Read Inflation
    # --------------------------------------------------

    inflation_df = (
        spark.read
        .option("multiLine", True)
        .json(INFLATION_PATH)
    )

    inflation_clean = transform_indicator(
        inflation_df,
        "INFLATION"
    )

    # --------------------------------------------------
    # Read Unemployment
    # --------------------------------------------------

    unemployment_df = (
        spark.read
        .option("multiLine", True)
        .json(UNEMPLOYMENT_PATH)
    )

    unemployment_clean = transform_indicator(
        unemployment_df,
        "UNEMPLOYMENT"
    )

    # --------------------------------------------------
    # Combine all indicators
    # --------------------------------------------------

    macro_df = (
        gdp_clean
        .unionByName(inflation_clean)
        .unionByName(unemployment_clean)
    )

    # --------------------------------------------------
    # Keep India only
    # --------------------------------------------------

    macro_df = macro_df.filter(
        F.col("country_code") == "IND"
    )

    # --------------------------------------------------
    # Show transformed data
    # --------------------------------------------------

    print("Transformed macro data:")

    macro_df.orderBy(
        F.desc("year"),
        "indicator_name"
    ).show(
        20,
        truncate=False
    )

    # --------------------------------------------------
    # Write Silver
    # --------------------------------------------------

    (
        macro_df.write
        .mode("overwrite")
        .partitionBy(
            "indicator_name"
        )
        .parquet(
            SILVER_PATH
        )
    )

    print(
        f"Macro Silver records: "
        f"{macro_df.count()}"
    )

    spark.stop()


if __name__ == "__main__":
    main()