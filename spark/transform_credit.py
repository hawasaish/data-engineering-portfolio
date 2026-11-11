from pyspark.sql import functions as F

from spark.spark_session import create_spark_session


CREDIT_FILE = (
    "data/raw/credit/credit_card_default.csv"
)

SILVER_PATH = (
    "data/silver/credit/"
)


def main():

    spark = create_spark_session()

    # --------------------------------------------------
    # Read raw credit dataset
    # --------------------------------------------------

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(CREDIT_FILE)
    )

    print("Raw credit schema:")
    df.printSchema()

    print("Raw credit records:")
    print(df.count())

    # --------------------------------------------------
    # Standardise column names
    # --------------------------------------------------

    df = df.toDF(
        *[
            column.strip()
            .lower()
            .replace(".", "_")
            .replace(" ", "_")
            for column in df.columns
        ]
    )

    # --------------------------------------------------
    # Rename source columns
    # --------------------------------------------------

    df = (
        df
        .withColumnRenamed(
            "id",
            "customer_id"
        )
        .withColumnRenamed(
            "limit_bal",
            "credit_limit"
        )
        .withColumnRenamed(
            "sex",
            "gender"
        )
        .withColumnRenamed(
            "education",
            "education_level"
        )
        .withColumnRenamed(
            "marriage",
            "marital_status"
        )
        .withColumnRenamed(
            "default_payment_next_month",
            "default_flag"
        )
    )

    # --------------------------------------------------
    # Remove duplicate customers
    # --------------------------------------------------

    df = df.dropDuplicates(
        ["customer_id"]
    )

    # --------------------------------------------------
    # Basic data quality filters
    # --------------------------------------------------

    df = df.filter(
        F.col("customer_id").isNotNull()
    )

    df = df.filter(
        F.col("credit_limit") >= 0
    )

    df = df.filter(
        F.col("age") > 0
    )

    # --------------------------------------------------
    # Replace invalid/missing categorical values
    # --------------------------------------------------

    df = df.withColumn(
        "education_level",
        F.when(
            F.col("education_level").isNull(),
            0
        ).otherwise(
            F.col("education_level")
        )
    )

    df = df.withColumn(
        "marital_status",
        F.when(
            F.col("marital_status").isNull(),
            0
        ).otherwise(
            F.col("marital_status")
        )
    )

    # --------------------------------------------------
    # Total bill amount
    # --------------------------------------------------

    bill_columns = [
        "bill_amt1",
        "bill_amt2",
        "bill_amt3",
        "bill_amt4",
        "bill_amt5",
        "bill_amt6",
    ]

    payment_columns = [
        "pay_amt1",
        "pay_amt2",
        "pay_amt3",
        "pay_amt4",
        "pay_amt5",
        "pay_amt6",
    ]

    df = df.withColumn(
        "total_bill_amount",
        sum(
            F.coalesce(
                F.col(column),
                F.lit(0)
            )
            for column in bill_columns
        )
    )

    # --------------------------------------------------
    # Total payment amount
    # --------------------------------------------------

    df = df.withColumn(
        "total_payment_amount",
        sum(
            F.coalesce(
                F.col(column),
                F.lit(0)
            )
            for column in payment_columns
        )
    )

    # --------------------------------------------------
    # Average bill
    # --------------------------------------------------

    df = df.withColumn(
        "avg_bill_amount",
        F.col("total_bill_amount") / 6
    )

    # --------------------------------------------------
    # Average payment
    # --------------------------------------------------

    df = df.withColumn(
        "avg_payment_amount",
        F.col("total_payment_amount") / 6
    )

    # --------------------------------------------------
    # Credit utilisation
    # --------------------------------------------------

    df = df.withColumn(
        "credit_utilisation",
        F.when(
            (F.col("credit_limit") > 0) &
            (F.col("total_bill_amount") >= 0),
            F.col("total_bill_amount") / F.col("credit_limit")
        ).otherwise(
            F.lit(0.0)
        )
    )

    # --------------------------------------------------
    # Payment ratio
    # --------------------------------------------------

    df = df.withColumn(
        "payment_ratio",
        F.when(
            F.col("total_bill_amount") > 0,
            F.col("total_payment_amount")
            / F.col("total_bill_amount")
        ).otherwise(
            F.lit(1.0)
        )
    )

    # --------------------------------------------------
    # Age group
    # --------------------------------------------------

    df = df.withColumn(
        "age_group",
        F.when(
            F.col("age") < 30,
            "18-29"
        )
        .when(
            F.col("age") < 40,
            "30-39"
        )
        .when(
            F.col("age") < 50,
            "40-49"
        )
        .when(
            F.col("age") < 60,
            "50-59"
        )
        .otherwise(
            "60+"
        )
    )

    # --------------------------------------------------
    # Select final Silver columns
    # --------------------------------------------------

    silver_df = df.select(
        "customer_id",
        "credit_limit",
        "gender",
        "education_level",
        "marital_status",
        "age",
        "age_group",

        "pay_0",
        "pay_2",
        "pay_3",
        "pay_4",
        "pay_5",
        "pay_6",

        "bill_amt1",
        "bill_amt2",
        "bill_amt3",
        "bill_amt4",
        "bill_amt5",
        "bill_amt6",

        "pay_amt1",
        "pay_amt2",
        "pay_amt3",
        "pay_amt4",
        "pay_amt5",
        "pay_amt6",

        "total_bill_amount",
        "total_payment_amount",
        "avg_bill_amount",
        "avg_payment_amount",
        "credit_utilisation",
        "payment_ratio",

        "default_flag",
    )

    # --------------------------------------------------
    # Write Silver Parquet
    # --------------------------------------------------

    (
        silver_df.write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

    print(
        f"Silver credit records: "
        f"{silver_df.count()}"
    )

    silver_df.show(
        5,
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()