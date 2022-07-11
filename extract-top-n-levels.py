"""
Sample script that can run either directly in the workbench or as a job in a cluster.

It shows how you can extract the top #N levels of a product from a MARKET DATA FEED and upload the files in a S3 bucket.

"""

import datetime
import logging
import os

import boto3
import maystreet_data as md
from dask import bag


## The products that we want to extract
PRODUCTS = [
    "AAPL",
    "TSLA",
    "IBM",
    "F",
]

## Market data feed from which the top #N levels will be extracted.
MARKET_DATA_FEED = "xdp_nyse_integrated"
NUM_LEVELS = 5

## Day that we will fetch data for
DATE = datetime.date.today() - datetime.timedelta(days=20)

## The S3 bucket where you want to upload results to. This is the simple bucket name, without the "s3://" prefix.
## If you don't have a bucket, you can create one integrated with the Analytics Workbench from the launcher.
S3_BUCKET = ""

## AWS access details.
## If you want to upload to a bucket in your own AWS account outside of the Analytics Workbench, you will need to specify them.
## If the bucket was created and permissioned using the  Analytics Workbench launcher, you can leave the values as None.
AWS_ACCESS_KEY_ID = None
AWS_SECRET_ACCESS_KEY = None
AWS_SESSION_TOKEN = None

s3_client = boto3.client(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
)


def upload_to_aws(file_to_upload):
    file_name = os.path.basename(file_to_upload)
    s3_client.upload_file(file_to_upload, S3_BUCKET, file_name)


def get_columns():
    """
    Return columns requested for the top #N levels.
    """

    levels = [
        "feed",
        "product",
        "lastreceipttimestamp",
        "lastexchangetimestamp",
        "lastsequencenumber",
    ]
    levels_columns = ["bidprice_", "bidquantity_", "askprice_", "askquantity_"]
    for i in range(1, NUM_LEVELS + 1):
        for column in levels_columns:
            levels.append(f"{column}{i}")

    return ",".join(levels)


def fetch_rows(product):
    """
    Returns the Top #N levels for a product from the MARKET_DATA_FEED feed.
    """

    # We query the data lake by passing a SQL query to maystreet_data.query
    # Note that when we filter by month/day, they need to be 0-padded strings,
    # e.g. January is '01' and not 1.
    # The columns to be extracted are retrieved using the get_columns() function

    query = f"""
    SELECT
        {get_columns()}
    FROM
        "prod_lake"."p_mst_data_lake".mt_aggregated_price_update
    WHERE
        f = '{MARKET_DATA_FEED}'
        AND y = '{DATE.year}'
        AND m = '{str(DATE.month).rjust(2, '0')}'
        AND d = '{str(DATE.day).rjust(2, '0')}'
        AND product = '{product}'
    ORDER BY
        lastreceipttimestamp
    LIMIT 100
    """

    return product, list(md.query(md.DataSource.DATA_LAKE, query))


def export(arg):
    """
    Exports the results from fetch_row() in a custom CSV format.
    """

    symbol, rows = arg

    # The file were results will be stored.
    # It needs to be under /home/workbench, as that location is in a shared filesystem between
    # the cluster and the workbench. Any other location is not persistent.
    filename = f"/home/workbench/top_{NUM_LEVELS}-{DATE.isoformat()}-{symbol}.csv"

    with open(filename, "w+", newline="") as export_file:
        # Writing the retrieved the line to an output file.
        export_file.write(get_columns())
        export_file.write("\n")
        for row in rows:
            row_str = ",".join([str(i) for i in row.values()])
            row_str = row_str.replace("None", "")
            export_file.write(row_str)
            export_file.write("\n")

    return filename, len(rows)


if __name__ == "__main__":
    # We can use the standard Python logging library without any extra
    # configuration necessary: logs will be stored to disk when running as a job.
    logging.info(f'Exporting {", ".join(PRODUCTS)} for {DATE.isoformat()}...')

    # Run the exports in parallel if we're running in a cluster:
    # in this case the export code is not particularly CPU or memory intensive,
    # More complex export jobs would benefit more from a cluster.
    exported = bag.from_sequence(PRODUCTS).map(fetch_rows).map(export).compute()

    # Upload the generated files
    for filename, n_exported in exported:
        logging.info(f"Uploading {n_exported} records from {filename}")
        upload_to_aws(filename)

    logging.info("Done")
