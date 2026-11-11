CREATE SCHEMA IF NOT EXISTS risk;


-- =====================================================
-- Customer Dimension
-- =====================================================

CREATE TABLE IF NOT EXISTS risk.dim_customer (

    customer_key BIGSERIAL PRIMARY KEY,

    customer_id INTEGER UNIQUE NOT NULL,

    gender INTEGER,

    education_level INTEGER,

    marital_status INTEGER,

    age INTEGER,

    age_group VARCHAR(20)
);


-- =====================================================
-- Risk Dimension
-- =====================================================

CREATE TABLE IF NOT EXISTS risk.dim_risk (

    risk_key SERIAL PRIMARY KEY,

    risk_level VARCHAR(20) UNIQUE NOT NULL,

    risk_description TEXT
);


-- =====================================================
-- Economic Dimension
-- =====================================================

CREATE TABLE IF NOT EXISTS risk.dim_economic (

    economic_key BIGSERIAL PRIMARY KEY,

    country_code VARCHAR(10),

    country_name VARCHAR(200),

    year INTEGER,

    gdp_growth DOUBLE PRECISION,

    inflation DOUBLE PRECISION,

    unemployment DOUBLE PRECISION,

    UNIQUE (
        country_code,
        year
    )
);


-- =====================================================
-- Customer Risk Fact
-- =====================================================

CREATE TABLE IF NOT EXISTS risk.fact_customer_risk (

    customer_key BIGINT
        REFERENCES risk.dim_customer(customer_key),

    risk_key INTEGER
        REFERENCES risk.dim_risk(risk_key),

    economic_key BIGINT
        REFERENCES risk.dim_economic(economic_key),

    credit_limit DOUBLE PRECISION,

    total_bill_amount DOUBLE PRECISION,

    total_payment_amount DOUBLE PRECISION,

    avg_bill_amount DOUBLE PRECISION,

    avg_payment_amount DOUBLE PRECISION,

    credit_utilisation DOUBLE PRECISION,

    payment_ratio DOUBLE PRECISION,

    delinquency_count INTEGER,

    avg_payment_status DOUBLE PRECISION,

    high_utilisation_flag INTEGER,

    low_payment_ratio_flag INTEGER,

    risk_score DOUBLE PRECISION,

    default_flag INTEGER
);