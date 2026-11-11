import requests


def test_world_bank_api():

    url = (
        "https://api.worldbank.org/v2/"
        "country/IND/"
        "indicator/"
        "NY.GDP.MKTP.KD.ZG"
    )

    response = requests.get(
        url,

        params={
            "format": "json",
            "per_page": 5
        },

        timeout=30
    )

    assert response.status_code == 200

    payload = response.json()

    assert isinstance(
        payload,
        list
    )

    assert len(payload) >= 2