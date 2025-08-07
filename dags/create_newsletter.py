import os
import requests
import numpy as np

from airflow.sdk import asset, Asset, Metadata
from airflow.io.path import ObjectStoragePath

# Environment variable configuration
OBJECT_STORAGE_SYSTEM = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
OBJECT_STORAGE_CONN_ID = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv(
    "OBJECT_STORAGE_PATH_NEWSLETTER", default="include/newsletter"
)


@asset(schedule="@daily")
def raw_zen_quotes(context: dict):
    """
    Extracts a random set of quotes.
    """
    r = requests.get("https://zenquotes.io/api/quotes/random")
    quotes = r.json()

    run_date = context["dag_run"].logical_date.strftime("%Y-%m-%d")
    yield Metadata(Asset("raw_zen_quotes"), {"run_date": run_date})
    return quotes


@asset(schedule=[raw_zen_quotes])
def selected_quotes(context: dict):
    """
    Transforms the extracted raw_zen_quotes.
    """
    raw_zen_quotes = context["ti"].xcom_pull(
        dag_id="raw_zen_quotes",
        task_ids="raw_zen_quotes",
        key="return_value",
        include_prior_dates=True,
    )

    quotes_character_counts = [int(quote["c"]) for quote in raw_zen_quotes]
    median = np.median(quotes_character_counts)

    median_quote = min(raw_zen_quotes, key=lambda quote: abs(int(quote["c"]) - median))
    raw_zen_quotes.remove(median_quote)

    short_quote = next(quote for quote in raw_zen_quotes if int(quote["c"]) < median)
    long_quote = next(quote for quote in raw_zen_quotes_
