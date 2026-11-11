from pathlib import Path
import pandas as pd


INPUT_FILE = Path(
    "data/raw/credit/default_of_credit_card_clients.xls"
)

OUTPUT_FILE = Path(
    "data/raw/credit/credit_card_default.csv"
)


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Dataset not found: {INPUT_FILE}"
        )

    df = pd.read_excel(
        INPUT_FILE,
        header=1
    )

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_csv(
        OUTPUT_FILE,
        index=False
    )

    print(f"Records: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()