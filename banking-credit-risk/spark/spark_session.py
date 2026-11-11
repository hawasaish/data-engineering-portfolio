from pyspark.sql import SparkSession


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("BankingCreditRisk")

        # ---------------------------------------------
        # S3A / MinIO Configuration
        # ---------------------------------------------

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://localhost:9000"
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            "minioadmin"
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "minioadmin123"
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark