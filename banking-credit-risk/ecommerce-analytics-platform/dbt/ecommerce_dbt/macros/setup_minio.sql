{% macro setup_minio() %}

    CREATE OR REPLACE SECRET minio_secret (
        TYPE S3,
        KEY_ID '{{ env_var("MINIO_ACCESS_KEY", "minioadmin") }}',
        SECRET '{{ env_var("MINIO_SECRET_KEY", "minioadmin") }}',
        ENDPOINT '{{ env_var("MINIO_ENDPOINT", "localhost:9000") }}',
        USE_SSL false,
        URL_STYLE 'path'
    );

{% endmacro %}