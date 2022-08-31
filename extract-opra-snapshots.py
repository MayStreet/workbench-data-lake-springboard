"""
Sample script that can run either directly in the workbench or as a job in a cluster.

It shows how you can extract the 1 minute trade snapshots for an underlying from the OPRA feed.

"""

import datetime
import logging

from dask import bag
import maystreet_data as md


## The underlying products for which we want to extract the data
UNDERLYING = ["SPY"]

## The market data feed to use to extract the 1 minute snapshots.
MARKET_DATA_FEED = "opra"

## Day that we will fetch data for
DATE = datetime.date.today() - datetime.timedelta(days=20)


def fetch_rows(product):
    """
    Returns 1 minute trade snapshots of options based on the product.
    """

    query = f"""
        WITH
            lake AS (
                SELECT
                    *
                FROM
                    "prod_lake"."p_mst_data_lake"."mt_trade"
                WHERE
                    f = '{MARKET_DATA_FEED}'
                    AND dt = '{DATE.isoformat()}'
                    AND product LIKE '{product}%'
            ),
            price_and_time AS (
                SELECT
                    MAX(SequenceNumber) SequenceNumber,
                    Product,
                    DATE_TRUNC('minute', TO_TIMESTAMP(ExchangeTimestamp / 1000000000)) AS dp_minute
                FROM
                    lake
                GROUP BY
                    DATE_TRUNC('minute', TO_TIMESTAMP(ExchangeTimestamp / 1000000000)),
                    product,
                    dt
            )
        SELECT
            dp_minute,
            lake.*
        FROM
            lake
                JOIN
            price_and_time USING (SequenceNumber, Product)
        WHERE
            dt = '{DATE.isoformat()}'
        ORDER BY
            dp_minute
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
    filename = f"/home/workbench/snap_1S_{DATE.isoformat()}-{symbol}.csv"

    with open(filename, "w+", newline="") as export_file:
        # Writing the retrieved the line to an output file.
        header = ""
        for row in rows:
            if header == "":
                header += "dp_minute"
                header = ",".join([str(i) for i in row.keys()])
                export_file.write(header + "\n")

            row_str = ",".join([str(i) for i in row.values()])
            row_str = row_str.replace("None", "")
            export_file.write(row_str + "\n")

    return len(rows)


if __name__ == "__main__":
    # We can use the standard Python logging library without any extra
    # configuration necessary: logs will be stored to disk when running as a job.
    logging.info(f'Exporting {", ".join(UNDERLYING)} for {DATE.isoformat()}...')

    # Run the exports in parallel if we're running in a cluster:
    # in this case the export code is not particularly CPU or memory intensive,
    # More complex export jobs would benefit more from a cluster.
    n_exported = bag.from_sequence(UNDERLYING).map(fetch_rows).map(export).compute()

    # We can sum together the statistics collected by each worker.
    logging.info(f"Exported {sum(n_exported)} prices.")
