"""
Formats the newsletter.
"""
from airflow.io.path import ObjectStoragePath

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

# fetch the run date of the pipeline
run_date = context["triggering_asset_events"][Asset("selected_quotes")][0].extra[
   "run_date"
]

newsletter_template_path = object_storage_path / "newsletter_template.txt"

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

# attach the run date to the asset event
yield Metadata(Asset("formatted_newsletter"), {"run_date": run_date})
