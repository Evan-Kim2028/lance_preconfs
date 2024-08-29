import marimo

__generated_with = "0.8.5"
app = marimo.App(
    width="full",
    app_title="preconf_analytics",
    layout_file="layouts/test_app.grid.json",
)


@app.cell
def __():
    import marimo as mo
    import polars as pl
    from lancedb_tables.lance_table import LanceTable

    pl.Config.set_fmt_str_lengths(200)
    pl.Config.set_fmt_float("full")
    return LanceTable, mo, pl


@app.cell(hide_code=True)
def __(LanceTable):
    # Lance table info
    commitment_table_name = "commitments"
    index: str = "block_number"
    lance_tables = LanceTable()
    uri: str = "data"  # locally saved to "data folder"

    # open the database and get the latest block_number
    commitments_table = lance_tables.open_table(
        uri=uri, table=commitment_table_name
    )
    return (
        commitment_table_name,
        commitments_table,
        index,
        lance_tables,
        uri,
    )


@app.cell
def __(commitments_table, pl):
    df = (
        pl.from_arrow(commitments_table.to_lance().to_table())
        .with_columns(
            (pl.col("dispatchTimestamp") - pl.col("decayStartTimeStamp")).alias(
                "bid_decay_latency"
            ),
            (pl.col("bid") / 10**18).alias("bid_eth"),
            pl.from_epoch("dispatchTimestamp", time_unit="ms").alias("datetime"),
        )
        .select(
            "datetime",
            "bid_decay_latency",
            "isSlash",
            "block_number",
            "blockNumber",
            "txnHash",
            "bid_eth",
            "commiter",
            "bidder",
        )
        .rename(
            {
                "block_number": "mev_commit_block_number",
                "blockNumber": "l1_block_number",
                "txnHash": "l1_txnHash",
            }
        )
    )
    return df,


@app.cell
def __(mo):
    mo.md("# Preconf Analytics")
    return


@app.cell
def __(mo):
    mo.md(
        "### Bid Latency - Search transactions that are within a bid latency threshold. Filter each column further by clicking on the column name."
    )
    return


@app.cell
def __(mo):
    # Cell 2 - create a filter
    bid_latency_filter = mo.ui.slider(
        start=0, stop=1000, value=50, label="Bid Latency"
    )
    bid_latency_filter
    return bid_latency_filter,


@app.cell
def __(bid_latency_filter, df, mo, pl):
    # Cell 3 - display the transformed dataframe
    filtered_df = df.filter(pl.col("bid_decay_latency") < bid_latency_filter.value)
    mo.ui.table(filtered_df)
    return filtered_df,


if __name__ == "__main__":
    app.run()
