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
    r = requests.get("https://zenquotes.io/api/quotes/random")  # This only returns one quote!
    quotes = r.json()

    if not isinstance(quotes, list) or len(quotes) < 3:
        raise ValueError(f"Expected multiple quotes but got: {quotes}")

    run_date = context["dag_run"].logical_date.strftime("%Y-%m-%d")
    yield Metadata(Asset("raw_zen_quotes"), {"run_date": run_date})
    return quotes


@asset(schedule=[raw_zen_quotes])
def selected_quotes(context: dict):
    """
    Transforms the extracted raw_zen_quotes.
    Selects a short, median, and long quote based on character count.
    """
    raw_zen_quotes = context["ti"].xcom_pull(
        dag_id="raw_zen_quotes",
        task_ids="raw_zen_quotes",
        key="return_value",
        include_prior_dates=True,
    )

    if not raw_zen_quotes or len(raw_zen_quotes) < 3:
        raise ValueError("Need at least 3 quotes to build the newsletter.")

    quotes_character_counts = [int(q["c"]) for q in raw_zen_quotes]
    median = np.median(quotes_character_counts)

    # Median quote: closest to median character count
    median_quote = min(raw_zen_quotes, key=lambda q: abs(int(q["c"]) - median))
    raw_zen_quotes.remove(median_quote)

    short_quotes = [q for q in raw_zen_quotes if int(q["c"]) < median]
    long_quotes = [q for q in raw_zen_quotes if int(q["c"]) > median]

    if not short_quotes or not long_quotes:
        raise ValueError(
            f"Not enough variety of quote lengths. short={len(short_quotes)}, long={len(long_quotes)}"
        )

    short_quote = short_quotes[0]
    long_quote = long_quotes[0]

    run_date = context["triggering_asset_events"][Asset("raw_zen_quotes")][0].extra[
        "run_date"
    ]
    yield Metadata(Asset("selected_quotes"), {"run_date": run_date})

    return {
        "median_q": median_quote,
        "short_q": short_quote,
        "long_q": long_quote,
    }


@asset(schedule=[selected_quotes])
def formatted_newsletter(context: dict):
    """
    Formats and stores the newsletter using the selected quotes.
    """
    object_storage_path = ObjectStoragePath(
        f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_NEWSLETTER}",
        conn_id=OBJECT_STORAGE_CONN_ID,
    )

    selected_quotes = context["ti"].xcom_pull(
        dag_id="selected_quotes",
        task_ids="selected_quotes",
        key="return_value",
        include_prior_dates=True,
    )

    if not selected_quotes:
        raise ValueError("selected_quotes returned no data")

    run_date = context["triggering_asset_events"][Asset("selected_quotes")][0].extra[
        "run_date"
    ]

    newsletter_template_path = object_storage_path / "newsletter_template.txt"
    if not newsletter_template_path.exists():
        raise FileNotFoundError(f"Newsletter template not found at {newsletter_template_path}")

    newsletter_template = newsletter_template_path.read_text()

    newsletter = newsletter_template.format(
        quote_text_1=selected_quotes["short_q"]["q"],
        quote_author_1=selected_quotes["short_q"]["a"],
        quote_text_2=selected_quotes["median_q"]["q"],
        quote_author_2=selected_quotes["median_q"]["a"],
        quote_text_3=selected_quotes["long_q"]["q"],
        quote_author_3=selected_quotes["long_q"]["a"],
        date=run_date,
    )

    date_newsletter_path = object_storage_path / f"{run_date}_newsletter.txt"
    date_newsletter_path.write_text(newsletter)

    yield Metadata(Asset("formatted_newsletter"), {"run_date": run_date})
    return newsletter
