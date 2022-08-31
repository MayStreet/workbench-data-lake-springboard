"""
Sample script that can run either directly in the workbench or as a job in a cluster.

It shows how you can export data in a custom format.

It extracts the time and price of each trade for 4 products (AAPL, TSLA, IBM, F) of the previous day,
and stores the data in the filesystem shared between the workbench instance and the clusters.

The export format is one CSV file for each product/day combination.
"""

import csv
import datetime
import logging

from dask import bag
import maystreet_data as md


## The products that we want to extract
PRODUCTS = [
    "AAPL",
    "TSLA",
    "IBM",
    "F",
]
## Day that we will fetch data for
DATE = datetime.date.today() - datetime.timedelta(days=1)


def fetch_rows(product):
    """
    Returns the product and a list of timestamp and price for the given product in the current DATE,
    ordered by timestamp.
    """

    # We query the data lake by passing a SQL query to maystreet_data.query
    # Note that when we filter by month/day, they need to be 0-padded strings,
    # e.g. January is '01' and not 1.
    query = f"""
    SELECT
        ExchangeTimestamp AS ts,
        price
    FROM
        "prod_lake"."p_mst_data_lake".mt_trade
    WHERE
        dt = '{DATE.isoformat()}'
        AND product = '{product}'
    ORDER BY
        ExchangeTimestamp
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
    filename = f"/home/workbench/custom-export-{DATE.isoformat()}-{symbol}.csv"

    n_exported = 0

    with open(filename, "w+", newline="") as export_file:
        # use the standard python CSV library to generate the exported file
        rows_writer = csv.writer(
            export_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        rows_writer.writerow(["Time", "Price"])
        for row in rows:
            # the timestamp is an integer divide by 1000000000 to get a unix timestamp that
            # can then be converted to a Python datetime
            timestamp = datetime.datetime.fromtimestamp(row["ts"] / 1000000000)
            formatted_timestamp = timestamp.strftime("%H:%M:%S")
            rows_writer.writerow([formatted_timestamp, row["price"]])

            if n_exported % 100000 == 0:
                logging.info(f"Exported {n_exported} rows for {symbol}")

            n_exported += 1

    return n_exported


if __name__ == "__main__":
    # We can use the standard Python logging library without any extra
    # configuration necessary: logs will be stored to disk when running as a job.
    logging.info(f'Exporting {", ".join(PRODUCTS)} for {DATE.isoformat()}...')

    # Run the exports in parallel if we're running in a cluster:
    # in this case the export code is not particularly CPU or memory intensive,
    # More complex export jobs would benefit more from a cluster.
    n_exported = bag.from_sequence(PRODUCTS).map(fetch_rows).map(export).compute()

    # We can sum together the statistics collected by each worker.
    logging.info(f"Exported {sum(n_exported)} prices.")
