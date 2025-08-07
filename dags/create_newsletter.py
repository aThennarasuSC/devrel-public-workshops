from airflow.sdk import asset, Asset, Metadata
from airflow.io.path import ObjectStoragePath
import os

OBJECT_STORAGE_SYSTEM = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
OBJECT_STORAGE_CONN_ID = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv("OBJECT_STORAGE_PATH_NEWSLETTER", default="include/newsletter")

@asset(schedule=["selected_quotes"])
def formatted_newsletter(context: dict):
    """
    Formats the newsletter using the selected quotes and writes it to object storage.
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
        raise ValueError("No selected quotes data found")

    triggering_events = context["triggering_asset_events"].get(Asset("selected_quotes"), [])
    if not triggering_events:
        raise ValueError("No triggering asset events found for selected_quotes")

    run_date = triggering_events[0].extra.get("run_date")
    if not run_date:
        raise ValueError("Run date not found in asset event metadata")

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
