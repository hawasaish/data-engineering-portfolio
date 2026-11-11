import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests


BASE_URL = (
    "https://api.worldbank.org/v2"
)

COUNTRY = "IND"

INDICATORS = {
    "gdp_growth":
        "NY.GDP.MKTP.KD.ZG",

    "inflation":
        "FP.CPI.TOTL.ZG",

    "unemployment":
        "SL.UEM.TOTL.ZS",
}

STATE_FILE = Path(
    "config/api_state.json"
)

OUTPUT_DIR = Path(
    "data/raw/macro_api"
)


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "%(levelname)s "
        "%(message)s"
    )
)

logger = logging.getLogger(__name__)


def load_state():

    if not STATE_FILE.exists():

        return {
            indicator: 2005
            for indicator in INDICATORS
        }

    with STATE_FILE.open(
            "r",
            encoding="utf-8"
    ) as file:

        return json.load(file)


def save_state(state):

    STATE_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with STATE_FILE.open(
            "w",
            encoding="utf-8"
    ) as file:

        json.dump(
            state,
            file,
            indent=2
        )


def fetch_indicator(
        indicator_name,
        indicator_code,
        start_year,
        end_year
):

    url = (
        f"{BASE_URL}/country/"
        f"{COUNTRY}/indicator/"
        f"{indicator_code}"
    )

    params = {
        "format": "json",
        "per_page": 100,
        "date": (
            f"{start_year}:{end_year}"
        ),
    }

    logger.info(
        "Fetching %s: %s-%s",
        indicator_name,
        start_year,
        end_year
    )

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    if (
            not isinstance(payload, list)
            or len(payload) < 2
    ):

        raise ValueError(
            "Unexpected World Bank API response"
        )

    return payload


def save_response(
        indicator_name,
        payload
):

    timestamp = datetime.now(
        timezone.utc
    ).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    output_dir = (
            OUTPUT_DIR
            / indicator_name
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
            output_dir
            / f"{timestamp}.json"
    )

    with output_file.open(
            "w",
            encoding="utf-8"
    ) as file:

        json.dump(
            payload,
            file,
            indent=2
        )

    logger.info(
        "Saved %s",
        output_file
    )


def main():

    state = load_state()

    current_year = (
        datetime.now().year
    )

    changed = False

    for (
            indicator_name,
            indicator_code
    ) in INDICATORS.items():

        last_year = state.get(
            indicator_name,
            2005
        )

        start_year = (
                last_year + 1
        )

        if start_year > current_year:

            logger.info(
                "%s is already up to date.",
                indicator_name
            )

            continue

        payload = fetch_indicator(
            indicator_name,
            indicator_code,
            start_year,
            current_year
        )

        save_response(
            indicator_name,
            payload
        )

        state[indicator_name] = (
            current_year
        )

        changed = True

    if changed:

        save_state(state)

    logger.info(
        "Incremental extraction completed."
    )


if __name__ == "__main__":
    main()