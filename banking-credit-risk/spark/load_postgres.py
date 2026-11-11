import os

import psycopg2
from dotenv import load_dotenv
from pyspark.sql import functions as F

from spark.spark_session import create_spark_session


load_dotenv()


POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost"
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5432"
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "banking_risk"
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "banking"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "banking"
)


CUSTOMER_RISK_PATH = (
    "data/gold/customer_risk"
)

MACRO_PATH = (
    "data/gold/macro_risk"
)


def get_connection():

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def load_risk_dimension(
        connection
):

    rows = [
        (
            "LOW",
            "Lower observed risk according to the scoring rules"
        ),
        (
            "MEDIUM",
            "Moderate observed risk according to the scoring rules"
        ),
        (
            "HIGH",
            "Higher observed risk according to the scoring rules"
        ),
    ]

    cursor = connection.cursor()

    cursor.executemany(
        """
        INSERT INTO risk.dim_risk
        (
            risk_level,
            risk_description
        )
        VALUES (%s, %s)

        ON CONFLICT (risk_level)
        DO UPDATE SET
            risk_description =
            EXCLUDED.risk_description
        """,
        rows
    )

    connection.commit()

    cursor.close()


def load_customer_dimension(
        connection,
        customer_df
):

    cursor = connection.cursor()

    rows = [
        (
            int(row.customer_id),
            int(row.gender)
            if row.gender is not None
            else None,
            int(row.education_level)
            if row.education_level is not None
            else None,
            int(row.marital_status)
            if row.marital_status is not None
            else None,
            int(row.age)
            if row.age is not None
            else None,
            row.age_group,
        )
        for row in customer_df.collect()
    ]

    cursor.executemany(
        """
        INSERT INTO risk.dim_customer
        (
            customer_id,
            gender,
            education_level,
            marital_status,
            age,
            age_group
        )
        VALUES (
            %s, %s, %s, %s, %s, %s
        )

        ON CONFLICT (customer_id)
        DO UPDATE SET
            gender = EXCLUDED.gender,
            education_level = EXCLUDED.education_level,
            marital_status = EXCLUDED.marital_status,
            age = EXCLUDED.age,
            age_group = EXCLUDED.age_group
        """,
        rows
    )

    connection.commit()

    cursor.close()


def load_economic_dimension(
        connection,
        macro_df
):

    cursor = connection.cursor()

    rows = [
        (
            row.country_code,
            row.country_name,
            int(row.year),
            row.GDP_GROWTH,
            row.INFLATION,
            row.UNEMPLOYMENT,
        )
        for row in macro_df.collect()
    ]

    cursor.executemany(
        """
        INSERT INTO risk.dim_economic
        (
            country_code,
            country_name,
            year,
            gdp_growth,
            inflation,
            unemployment
        )
        VALUES (
            %s, %s, %s, %s, %s, %s
        )

        ON CONFLICT (
            country_code,
            year
        )
        DO UPDATE SET
            country_name =
                EXCLUDED.country_name,

            gdp_growth =
                EXCLUDED.gdp_growth,

            inflation =
                EXCLUDED.inflation,

            unemployment =
                EXCLUDED.unemployment
        """,
        rows
    )

    connection.commit()

    cursor.close()


def load_fact_table(
        connection,
        fact_df
):

    cursor = connection.cursor()

    rows = [
        (
            int(row.customer_key),
            int(row.risk_key),
            int(row.economic_key),

            row.credit_limit,
            row.total_bill_amount,
            row.total_payment_amount,

            row.avg_bill_amount,
            row.avg_payment_amount,

            row.credit_utilisation,
            row.payment_ratio,

            int(row.delinquency_count),
            row.avg_payment_status,

            int(row.high_utilisation_flag),
            int(row.low_payment_ratio_flag),

            row.risk_score,
            int(row.default_flag),
        )
        for row in fact_df.collect()
    ]

    cursor.executemany(
        """
        INSERT INTO risk.fact_customer_risk
        (
            customer_key,
            risk_key,
            economic_key,

            credit_limit,
            total_bill_amount,
            total_payment_amount,

            avg_bill_amount,
            avg_payment_amount,

            credit_utilisation,
            payment_ratio,

            delinquency_count,
            avg_payment_status,

            high_utilisation_flag,
            low_payment_ratio_flag,

            risk_score,
            default_flag
        )
        VALUES (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s
        )
        """,
        rows
    )

    connection.commit()

    cursor.close()


def main():

    spark = create_spark_session()

    connection = get_connection()

    try:

        # ---------------------------------------------
        # Read Gold customer risk
        # ---------------------------------------------

        customer_risk_df = (
            spark.read
            .parquet(CUSTOMER_RISK_PATH)
        )

        # ---------------------------------------------
        # Read latest macro snapshot
        # ---------------------------------------------

        macro_df = (
            spark.read
            .parquet(MACRO_PATH)
        )

        # ---------------------------------------------
        # Risk dimension
        # ---------------------------------------------

        load_risk_dimension(
            connection
        )

        # ---------------------------------------------
        # Customer dimension
        # ---------------------------------------------

        customer_dimension_df = (
            customer_risk_df
            .select(
                "customer_id",
                "gender",
                "education_level",
                "marital_status",
                "age",
                "age_group"
            )
            .dropDuplicates(
                ["customer_id"]
            )
        )

        load_customer_dimension(
            connection,
            customer_dimension_df
        )

        # ---------------------------------------------
        # Economic dimension
        # ---------------------------------------------

        load_economic_dimension(
            connection,
            macro_df
        )

        # ---------------------------------------------
        # Fetch database keys
        # ---------------------------------------------

        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                customer_key,
                customer_id
            FROM risk.dim_customer
            """
        )

        customer_keys = dict(
            cursor.fetchall()
        )

        cursor.execute(
            """
            SELECT
                risk_key,
                risk_level
            FROM risk.dim_risk
            """
        )

        risk_keys = {
            risk_level: risk_key
            for risk_key, risk_level
            in cursor.fetchall()
        }

        cursor.execute(
            """
            SELECT
                economic_key,
                year
            FROM risk.dim_economic
            WHERE country_code = 'IND'
            """
        )

        economic_rows = cursor.fetchall()

        economic_keys = {
            year: economic_key
            for economic_key, year
            in economic_rows
        }

        cursor.close()

        # ---------------------------------------------
        # Determine latest macro year
        # ---------------------------------------------

        latest_year = (
            macro_df
            .agg(
                F.max("year")
                .alias("latest_year")
            )
            .collect()[0]["latest_year"]
        )

        economic_key = economic_keys[
            latest_year
        ]

        # ---------------------------------------------
        # Prepare fact data
        # ---------------------------------------------

        fact_rows = (
            customer_risk_df
            .collect()
        )

        fact_data = []

        for row in fact_rows:

            customer_key = customer_keys[
                int(row.customer_id)
            ]

            risk_key = risk_keys[
                row.risk_level
            ]

            fact_data.append(
                (
                    customer_key,
                    risk_key,
                    economic_key,

                    row.credit_limit,
                    row.total_bill_amount,
                    row.total_payment_amount,

                    row.avg_bill_amount,
                    row.avg_payment_amount,

                    row.credit_utilisation,
                    row.payment_ratio,

                    int(row.delinquency_count),
                    row.avg_payment_status,

                    int(row.high_utilisation_flag),
                    int(row.low_payment_ratio_flag),

                    row.risk_score,
                    int(row.default_flag),
                )
            )

        cursor = connection.cursor()

        # ---------------------------------------------
        # Reload fact table
        # ---------------------------------------------

        cursor.execute(
            """
            TRUNCATE TABLE
            risk.fact_customer_risk;
            """
        )

        cursor.executemany(
            """
            INSERT INTO risk.fact_customer_risk
            (
                customer_key,
                risk_key,
                economic_key,

                credit_limit,
                total_bill_amount,
                total_payment_amount,

                avg_bill_amount,
                avg_payment_amount,

                credit_utilisation,
                payment_ratio,

                delinquency_count,
                avg_payment_status,

                high_utilisation_flag,
                low_payment_ratio_flag,

                risk_score,
                default_flag
            )
            VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s
            )
            """,
            fact_data
        )

        connection.commit()

        cursor.close()

        print(
            "PostgreSQL load completed successfully."
        )

        print(
            f"Fact records loaded: "
            f"{len(fact_data)}"
        )

    finally:

        connection.close()
        spark.stop()


if __name__ == "__main__":
    main()