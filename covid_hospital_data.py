import urllib.request
from google.cloud import bigquery
from google.oauth2 import service_account
from io import BytesIO


def get_source_file():
    source_url = "https://healthdata.gov/sites/default/files/reported_hospital_capacity_admissions_facility-level_weekly_average_timeseries_20201207.csv"
    response = urllib.request.urlopen(source_url)
    data = response.read()

    return data


def get_bq_client():
    """Get BigQuery Client"""

    key_path = "key.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    return client


def upload_to_bq(data):
    client = get_bq_client()

    dataset_id = "raw"
    table_id = "covid_hospital_data_20201207"

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.field_delimiter = ","
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_TRUNCATE"

    f = BytesIO(data)
    job = client.load_table_from_file(f, table_ref, job_config=job_config)
    f.close()

    job.result()
    print("File successfully loaded")


def main():
    data = get_source_file()
    upload_to_bq(data)


if __name__ == "__main__":
    main()
