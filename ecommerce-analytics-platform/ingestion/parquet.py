from pathlib import Path


OUTPUT_DIR = Path("output")


def save_parquet(df, table_name):
    output_path = OUTPUT_DIR / "transactions"

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    file_path = output_path / f"{table_name}.parquet"

    df.to_parquet(
        file_path,
        engine="pyarrow",
        index=False
    )

    print(f"Parquet file created: {file_path}")

    return file_path