from spark.spark_session import create_spark_session


CREDIT_FILE = (
    "data/raw/credit/credit_card_default.csv"
)

GDP_PATH = (
    "s3a://banking-risk/bronze/macro_api/gdp_growth/"
)

INFLATION_PATH = (
    "s3a://banking-risk/bronze/macro_api/inflation/"
)

UNEMPLOYMENT_PATH = (
    "s3a://banking-risk/bronze/macro_api/unemployment/"
)


def main():

    spark = create_spark_session()

    # --------------------------------------------------
    # Credit Card Default Dataset
    # --------------------------------------------------

    credit_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(CREDIT_FILE)
    )

    print("Credit dataset:")
    credit_df.printSchema()

    credit_df.show(
        5,
        truncate=False
    )

    # --------------------------------------------------
    # GDP Growth API Dataset
    # --------------------------------------------------

    gdp_growth_df = (
        spark.read
        .option("multiLine", True)
        .json(GDP_PATH)
    )

    print("GDP Growth API dataset:")
    gdp_growth_df.printSchema()

    gdp_growth_df.show(
        5,
        truncate=False
    )

    # --------------------------------------------------
    # Inflation API Dataset
    # --------------------------------------------------

    inflation_df = (
        spark.read
        .option("multiLine", True)
        .json(INFLATION_PATH)
    )

    print("Inflation API dataset:")
    inflation_df.printSchema()

    inflation_df.show(
        5,
        truncate=False
    )

    # --------------------------------------------------
    # Unemployment API Dataset
    # --------------------------------------------------

    unemployment_df = (
        spark.read
        .option("multiLine", True)
        .json(UNEMPLOYMENT_PATH)
    )

    print("Unemployment API dataset:")
    unemployment_df.printSchema()

    unemployment_df.show(
        5,
        truncate=False
    )

    # --------------------------------------------------
    # Stop Spark
    # --------------------------------------------------

    spark.stop()


if __name__ == "__main__":
    main()