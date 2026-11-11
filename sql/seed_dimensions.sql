INSERT INTO risk.dim_risk (
    risk_level,
    risk_description
)
VALUES
(
    'LOW',
    'Lower observed risk according to the scoring rules'
),
(
    'MEDIUM',
    'Moderate observed risk according to the scoring rules'
),
(
    'HIGH',
    'Higher observed risk according to the scoring rules'
)
ON CONFLICT (risk_level)
DO UPDATE SET
    risk_description = EXCLUDED.risk_description;