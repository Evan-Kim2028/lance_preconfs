import marimo

__generated_with = "0.8.9"
app = marimo.App(width="full", app_title="preconf_analytics")


@app.cell(hide_code=True)
def __():
    import altair as alt
    import marimo as mo
    import pandas as pd
    import polars as pl

    from lancedb_tables.lance_table import LanceTable
    from datetime import datetime, timedelta

    pl.Config.set_fmt_str_lengths(200)
    pl.Config.set_fmt_float("full")
    None
    return LanceTable, alt, datetime, mo, pd, pl, timedelta


@app.cell(hide_code=True)
def __(LanceTable, pl):
    # Lance table info
    commitment_table_name: str = "commitments"
    l1_tx_table_name: str = "l1_txs"
    mev_boost_table_name: str = "mev_boost_blocks"
    index: str = "block_number"
    lance_tables = LanceTable()
    uri: str = "data"  # locally saved to "data folder"

    # open the mev-commit commitments table
    commitments_table = lance_tables.open_table(
        uri=uri, table=commitment_table_name
    )

    # open the l1 txs table
    l1_tx_table = _table = lance_tables.open_table(uri=uri, table=l1_tx_table_name)
    l1_tx_df = pl.from_arrow(l1_tx_table.to_lance().to_table())

    # open mev-boost-blocks table
    mev_boost_blocks = lance_tables.open_table(uri=uri, table=mev_boost_table_name)
    mev_boost_blocks_df = pl.from_arrow((mev_boost_blocks.to_lance().to_table()))
    return (
        commitment_table_name,
        commitments_table,
        index,
        l1_tx_df,
        l1_tx_table,
        l1_tx_table_name,
        lance_tables,
        mev_boost_blocks,
        mev_boost_blocks_df,
        mev_boost_table_name,
        uri,
    )


@app.cell(hide_code=True)
def __(commitments_table, pl):
    commit_df = (
        pl.from_arrow(commitments_table.to_lance().to_table())
        .with_columns(
            (pl.col("dispatchTimestamp") - pl.col("decayStartTimeStamp")).alias(
                "bid_decay_latency"
            ),
            (pl.col("bid") / 10**18).alias("bid_eth"),
            pl.from_epoch("timestamp", time_unit="ms").alias("datetime"),
        )
        # bid decay calculations
        # the formula to calculate the bid decay = (decayEndTimeStamp - decayStartTimeStamp) / (dispatchTimestamp - decayEndTimeStamp). If it's a negative number, then bid would have decayed to 0
        .with_columns(
            # need to change type from uint to int to account for negative numbers
            pl.col("decayStartTimeStamp").cast(pl.Int64),
            pl.col("decayEndTimeStamp").cast(pl.Int64),
            pl.col("dispatchTimestamp").cast(pl.Int64),
        )
        .with_columns(
            (pl.col("decayEndTimeStamp") - pl.col("decayStartTimeStamp")).alias(
                "decay_range"
            ),
            (pl.col("decayEndTimeStamp") - pl.col("dispatchTimestamp")).alias(
                "dispatch_range"
            ),
        )
        .with_columns(
            (pl.col("dispatch_range") / pl.col("decay_range")).alias(
                "decay_multiplier"
            )
        )
        .with_columns(
            pl.when(pl.col("decay_multiplier") < 0)
            .then(0)
            .otherwise(pl.col("decay_multiplier"))
        )
        # calculate decayed bid. The decay multiplier is the amount that the bid decays by.
        .with_columns(
            (pl.col("decay_multiplier") * pl.col("bid_eth")).alias(
                "decayed_bid_eth"
            )
        )
        .select(
            "datetime",
            "bid_decay_latency",
            "decay_multiplier",
            "isSlash",
            "block_number",
            "blockNumber",
            "txnHash",
            "bid_eth",
            "decayed_bid_eth",
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
        .sort(by="datetime", descending=True)
    )
    return commit_df,


@app.cell(hide_code=True)
def __(commit_df, l1_tx_df, pl):
    # join commits and l1 df together
    commits_l1_df = commit_df.join(
        l1_tx_df.rename({"hash": "l1_txnHash"}),
        on="l1_txnHash",
        how="left",
        suffix="_l1",
    ).with_columns(
        # calculate if the preconf block was the same as the l1 block the tx ended up in
        (pl.col("block_number") - pl.col("l1_block_number")).alias("l1_block_diff")
    )
    return commits_l1_df,


@app.cell
def __(mo):
    mo.md("""# Preconf Analytics""")
    return


@app.cell
def __(mo):
    mo.md("""## mev-boost data""")
    return


@app.cell(hide_code=True)
def __():
    def byte_to_string(hex_string):
        if hex_string == "0x":
            return ""
        # Remove the "0x" prefix and decode the hex string
        bytes_object = bytes.fromhex(hex_string[2:])
        try:
            human_readable_string = bytes_object.decode("utf-8")
        except UnicodeDecodeError:
            human_readable_string = bytes_object.decode("latin-1")
        return human_readable_string
    return byte_to_string,


@app.cell(hide_code=True)
def __(byte_to_string, mev_boost_blocks_df, pl):
    mev_boost_relay_transformed_df = mev_boost_blocks_df.with_columns(
        pl.from_epoch("timestamp", time_unit="s").alias("datetime"),
        # map byte_to_string
        pl.col("extra_data")
        .map_elements(byte_to_string, return_dtype=str)
        .alias("builder_graffiti"),
        pl.when(pl.col("relay").is_null())
        .then(False)
        .otherwise(True)
        .alias("mev_boost"),
        (pl.col("value") / 10**18).round(9).alias("block_bid_eth"),
    ).select(
        "datetime",
        "block_number",
        "builder_graffiti",
        "mev_boost",
        "relay",
        "block_bid_eth",
        "base_fee_per_gas",
        "gas_used",
    )
    return mev_boost_relay_transformed_df,


@app.cell(hide_code=True)
def __(commit_df, mev_boost_relay_transformed_df, pl):
    # transform commit_df to stsandardize to block level data
    preconf_blocks_grouped_df = (
        commit_df.select(
            "l1_block_number",
            "isSlash",
            "bid_eth",
            "decayed_bid_eth",
            "commiter",
            "bidder",
        )
        .group_by("l1_block_number")
        .agg(
            pl.col("decayed_bid_eth").sum().alias("total_decayed_bid_eth"),
            pl.col("bid_eth").sum().alias("total_bid_eth"),
        )
    )
    # join mev-boost data to preconf data
    mev_boost_blocks_preconfs_joined_df = mev_boost_relay_transformed_df.join(
        preconf_blocks_grouped_df,
        left_on="block_number",
        right_on="l1_block_number",
        how="left",
    ).with_columns(
        pl.when(pl.col("total_bid_eth").is_not_null())
        .then(True)
        .otherwise(False)
        .alias("preconf")
    )
    return mev_boost_blocks_preconfs_joined_df, preconf_blocks_grouped_df


@app.cell(hide_code=True)
def __(
    alt,
    mev_boost_blocks_preconfs_joined_df,
    mev_boost_relay_transformed_df,
    pl,
):
    min_mev_boost_block = (
        mev_boost_relay_transformed_df.tail(500)
        .select("block_number")
        .min()
        .item()
    )
    max_mev_boost_block = (
        mev_boost_relay_transformed_df.tail(500)
        .select("block_number")
        .max()
        .item()
    )


    mev_boost_block_chart = (
        alt.Chart(
            mev_boost_relay_transformed_df.tail(500)
            .group_by("mev_boost")
            .agg(pl.col("mev_boost").count().alias("count"))
        )
        .mark_bar()
        .encode(
            x=alt.X("mev_boost:N"),
            y=alt.Y("count:Q", title="Count"),
            color=alt.Color(
                "mev_boost:N", legend=alt.Legend(title="MEV Boost")
            ),  # Add legend
        )
        .properties(
            title=f"MEV-Boost Blocks from {min_mev_boost_block} - {max_mev_boost_block} ({max_mev_boost_block - min_mev_boost_block} blocks)",
            height=400,
            width=500,
        )
        .interactive()  # Enable interactivity
    )

    preconf_block_bids_chart = (
        alt.Chart(
            mev_boost_blocks_preconfs_joined_df.tail(500).filter(
                pl.col("block_bid_eth") > 0
            )
        )
        .mark_point()
        .encode(
            x="datetime:T",  # Ensure 'datetime' is treated as temporal data
            y="block_bid_eth:Q",
            color=alt.Color(
                "preconf:N",
                legend=alt.Legend(title="Preconf Block Bids"),
                scale=alt.Scale(
                    scheme="tableau10"
                ),  # Use a color scheme for improved colors
            ),
            tooltip=["block_number", "builder_graffiti", "block_bid_eth", "relay"],
        )
        .properties(title="mev-boost blocks with preconfs", height=400, width=500)
        .interactive()  # Enable interactivity
    )

    mev_boost_charts = (
        alt.concat(mev_boost_block_chart, preconf_block_bids_chart)
        .configure_view(continuousHeight=400, continuousWidth=600)
        .resolve_scale(
            color="independent"
        )  # Ensure each chart's legend is independent
    )

    mev_boost_charts.show()
    return (
        max_mev_boost_block,
        mev_boost_block_chart,
        mev_boost_charts,
        min_mev_boost_block,
        preconf_block_bids_chart,
    )


@app.cell(hide_code=True)
def __(mev_boost_blocks_preconfs_joined_df, pl):
    # chart that shows the total decayed bids vs the mev-boost bid amount
    preconf_bid_mev_boost_df = mev_boost_blocks_preconfs_joined_df.filter(
        pl.col("preconf") == True
    ).with_columns(
        (pl.col("total_decayed_bid_eth") / pl.col("block_bid_eth"))
        .round(3)
        .alias("preconf_bid_amt_pct")
    )
    return preconf_bid_mev_boost_df,


@app.cell(hide_code=True)
def __(alt, preconf_bid_mev_boost_df):
    # Create a scatter plot
    preconf_bid_breakdown = (
        alt.Chart(preconf_bid_mev_boost_df.tail(500))
        .mark_point()  # Mark type for scatter plot
        .encode(
            x=alt.X("datetime:T", title="datetime"),
            y=alt.Y("preconf_bid_amt_pct:Q", title="preconf percent of bid"),
            color=alt.Color("block_bid_eth:Q", title="mev-boost bid (ETH)").scale(
                scheme="blues"
            ),
            tooltip=[
                "block_number",
                "datetime",
                "builder_graffiti",
                "preconf_bid_amt_pct",
                "block_bid_eth",
            ],
        )
        .properties(
            width=600,
            height=400,
            title="preconf percent of mev-boost bids",
        )
        .interactive()
    )

    preconf_bid_breakdown
    return preconf_bid_breakdown,


@app.cell
def __(mo):
    mo.md(r"""## Bidder Activity""")
    return


@app.cell(hide_code=True)
def __(commits_l1_df, pl):
    bidder_group_df = (
        commits_l1_df.group_by("bidder")
        .agg(
            pl.len().alias("bid_count"),
            pl.col("bid_eth").sum().alias("total_eth_bids"),
            pl.col("decayed_bid_eth").sum().alias("total_decayed_eth_bids"),
        )
        .sort(by="bid_count", descending=True)
    )

    # Melt the DataFrame
    melted_bidder_df = bidder_group_df.unpivot(
        index=["bidder"],  # Columns to keep as identifiers
        on=["total_eth_bids", "total_decayed_eth_bids"],  # Columns to melt
        variable_name="bid_type",  # New column name for melted variable
        value_name="eth_bids",  # New column name for values
    )
    return bidder_group_df, melted_bidder_df


@app.cell(hide_code=True)
def __(alt, bidder_group_df, melted_bidder_df):
    # Bidder Activity Charts
    # Preconf Bid Count
    bidder_count_chart = (
        alt.Chart(bidder_group_df.head(10))
        .mark_bar()
        .encode(
            y=alt.Y("bidder:N", axis=None),  # Hide y-axis in the first chart
            x=alt.X(
                "bid_count:Q", title="Preconf Count", scale=alt.Scale(reverse=True)
            ),
        )
        .properties(width=500, height=300, title="Preconf Commitments")
    )

    # Create a grouped bar chart
    bidder_bids_chart = (
        alt.Chart(melted_bidder_df)
        .mark_bar()  # Bar mark
        .encode(
            y=alt.X(
                "bidder:N", axis=alt.Axis(title="Bidders")
            ),  # Categorical Bidders on the x-axis
            x=alt.Y(
                "eth_bids:Q", title="ETH Bids"
            ),  # Quantitative ETH Bids on y-axis
            color=alt.Color(
                "bid_type:N", legend=alt.Legend(title="Bid Type")
            ),  # Color represents bid type
            yOffset=alt.X("bid_type:N"),  # Offset bars by bid type for grouping
        )
        .properties(
            width=500,  # Width for the chart
            height=300,  # Adjust height for the chart
            title="Total Bids (ETH) by Bidder and Type",
        )
    )

    # Combine the two charts side-by-side with better spacing
    combined_chart = alt.concat(
        bidder_count_chart, bidder_bids_chart, spacing=0
    ).resolve_scale(
        y="shared"  # Share the y-axis scale between the two charts
    )

    combined_chart.show()
    return bidder_bids_chart, bidder_count_chart, combined_chart


@app.cell
def __(mo):
    mo.md("""## slash rate""")
    return


@app.cell(hide_code=True)
def __(commits_l1_df, pl):
    # Round the datetime column to the nearest hour
    date_truncate_df = commits_l1_df.with_columns(
        pl.col("datetime").dt.truncate("1h").alias("hour")
    )

    # Group by the rounded datetime and calculate the slash rate count
    slash_rate_df = date_truncate_df.group_by("hour").agg(
        [
            pl.col("isSlash")
            .sum()
            .alias("slash_count"),  # Count of True in 'isSlash'
            pl.len().alias("total_count"),  # Total count per hour
        ]
    )

    # Add a new column for the slash rate (optional)
    slash_rate_df = slash_rate_df.with_columns(
        (pl.col("slash_count") / pl.col("total_count")).alias("slash_rate"),
        (pl.col("total_count") - pl.col("slash_count")).alias("non_slash_count"),
    )
    return date_truncate_df, slash_rate_df


@app.cell(hide_code=True)
def __(alt, datetime, pd, pl, slash_rate_df, timedelta):
    # Calculate data for the past 24 hours
    current_time = datetime.now()
    past_24_hours = current_time - timedelta(hours=24)

    # Filter for the past 24 hours
    df_last_24_hours = slash_rate_df.filter(pl.col("hour") >= past_24_hours)

    # Calculate total slash count and rate for the past 24 hours
    total_slash_count = df_last_24_hours["slash_count"].sum()
    total_non_slash_count = df_last_24_hours["non_slash_count"].sum()
    # Check for None type and set total_slash_rate to 0 if None
    total_slash_rate = df_last_24_hours["slash_rate"].mean()
    if total_slash_rate is None:
        total_slash_rate = 0


    # Create formatted strings for each line of the text box
    summary_text = [
        "Slashing Summary (past 24 hours)",
        f"Non Slash Count: {total_non_slash_count}",
        f"Slash Count: {total_slash_count}",
        f"Slash Rate: {total_slash_rate * 100:.2f}%",
    ]
    # Create a common color encoding with legend to distinguish the bar and line charts
    color = alt.Color("Metric:N", legend=alt.Legend(title="Metrics"))

    # Bar chart for daily slash count
    slash_count_bar_chart = (
        alt.Chart(slash_rate_df)
        .transform_calculate(Metric='"Slash Count"')
        .mark_bar()
        .encode(
            x=alt.X(
                "hour:T", title="Hour", axis=alt.Axis(labelAngle=45)
            ),  # Rotate x-axis labels by 45 degrees
            y=alt.Y("slash_count:Q", title="Slash Count"),
            color=color,  # Add color encoding for legend
            tooltip=[
                "hour:T",
                "slash_count:Q",
                "slash_rate:Q",
            ],  # Add tooltip to show details on hover
        )
        .properties(width=800, height=300)
    )

    # Line chart for slash rate over time with 50% transparency and legend
    slash_rate_line_chart = (
        alt.Chart(slash_rate_df)
        .transform_calculate(Metric='"Slash Rate"')
        .mark_line(opacity=0.35)
        .encode(
            x=alt.X(
                "hour:T", axis=alt.Axis(labelAngle=45)
            ),  # Ensure the x-axis rotation is consistent
            y=alt.Y("slash_rate:Q", title="Slash Rate"),
            color=color,  # Add color encoding for legend
            tooltip=["hour:T", "slash_rate:Q"],  # Tooltip for slash rate
        )
    )

    # Overlay the bar and line charts
    slash_rate_combined_chart = (
        alt.layer(slash_count_bar_chart, slash_rate_line_chart)
        .resolve_scale(
            y="independent"  # Allow each chart to have its own y-axis scale
        )
        .properties(title="Historical Slashing Rates")
    )

    # Text box for summary statistics
    slash_stats_text_box = (
        alt.Chart(pd.DataFrame({"text": [summary_text]}))
        .mark_text(
            align="left",
            baseline="top",
            dx=-50,
            dy=-150,
            fontSize=24,
            color="black",  # Font color
        )
        .encode(text="text:N")
        .properties(
            width=150,  # Width of the text box
            height=300,  # Match the height of the main chart
        )
    )

    # Combine the main chart with the text box using horizontal concatenation
    final_slashing_chart = (
        alt.hconcat(slash_stats_text_box, slash_rate_combined_chart).resolve_scale(
            y="independent"
        )
        # .properties(title="Slash Count and Slash Rate Over Time")
    )

    final_slashing_chart.show()
    return (
        color,
        current_time,
        df_last_24_hours,
        final_slashing_chart,
        past_24_hours,
        slash_count_bar_chart,
        slash_rate_combined_chart,
        slash_rate_line_chart,
        slash_stats_text_box,
        summary_text,
        total_non_slash_count,
        total_slash_count,
        total_slash_rate,
    )


@app.cell(hide_code=True)
def __(date_truncate_df, pl):
    # Group by the rounded datetime and calculate the slash rate count
    commiter_slash_rate_df = date_truncate_df.group_by("hour", "commiter").agg(
        [
            pl.col("isSlash")
            .sum()
            .alias("slash_count"),  # Count of True in 'isSlash'
            pl.len().alias("total_count"),  # Total count per hour
        ]
    )

    # Add a new column for the slash rate (optional)
    commiter_slash_rate_df = commiter_slash_rate_df.with_columns(
        (pl.col("slash_count") / pl.col("total_count")).alias("slash_rate"),
        (pl.col("total_count") - pl.col("slash_count")).alias("non_slash_count"),
    )
    return commiter_slash_rate_df,


@app.cell(hide_code=True)
def __(alt, color, commiter_slash_rate_df, past_24_hours, pl):
    # Melted commiter slashing dataframe (total history)
    historical_provider_slashing = commiter_slash_rate_df.unpivot(
        index=["hour", "commiter"],  # Columns to keep
        on=["slash_count", "non_slash_count"],  # Columns to melt
        variable_name="Metric",  # New column name for the metric names
        value_name="Count",  # New column name for the counts
    )

    # Filter the DataFrame for the past 24 hours
    filtered_provider_slashing_df = commiter_slash_rate_df.filter(
        pl.col("hour") >= past_24_hours
    )

    # Reshape the filtered DataFrame to a long format for the second stacked bar chart
    filtered_provider_slash_melted_df = filtered_provider_slashing_df.unpivot(
        index=["hour", "commiter"],  # Columns to keep
        on=["slash_count", "non_slash_count"],  # Columns to melt
        variable_name="Metric",  # New column name for the metric names
        value_name="Count",  # New column name for the counts
    )

    # Create the first stacked bar chart (already provided)
    stacked_bar_chart = (
        alt.Chart(historical_provider_slashing)
        .mark_bar()
        .encode(
            x=alt.X(
                "commiter:N", title="Provider"
            ),  # Categorical x-axis for committers
            y=alt.Y("Count:Q", title="Count"),  # Quantitative y-axis for counts
            color=color,  # Color encoding to stack by Metric
            tooltip=[
                "commiter:N",
                "Metric:N",
                "Count:Q",
            ],  # Add tooltip to show details on hover
        )
        .properties(width=600, height=300, title="Provider Slash Rate (Total)")
    )

    # Create the second stacked bar chart for the past 24 hours
    filtered_stacked_bar_chart = (
        alt.Chart(filtered_provider_slash_melted_df)
        .mark_bar()
        .encode(
            x=alt.X(
                "commiter:N", title="Provider (Last 24 Hours)"
            ),  # Categorical x-axis for committers
            y=alt.Y("Count:Q", title="Count"),  # Quantitative y-axis for counts
            color=color,  # Color encoding to stack by Metric
            tooltip=[
                "commiter:N",
                "Metric:N",
                "Count:Q",
            ],  # Add tooltip to show details on hover
        )
        .properties(width=600, height=300, title="Provider Slash Rate (24 hours)")
    )

    # Concatenate the two charts side by side
    provider_slashing_combined_chart = alt.hconcat(
        stacked_bar_chart, filtered_stacked_bar_chart
    )  # .resolve_scale(y='shared')

    provider_slashing_combined_chart.show()
    return (
        filtered_provider_slash_melted_df,
        filtered_provider_slashing_df,
        filtered_stacked_bar_chart,
        historical_provider_slashing,
        provider_slashing_combined_chart,
        stacked_bar_chart,
    )


@app.cell
def __(mo):
    mo.md("""## Opened Commitments Lookup""")
    return


@app.cell
def __(mo):
    mo.md("""Search for preconfirmation bids. Filter each column further by clicking on the column name.""")
    return


@app.cell(hide_code=True)
def __(commits_l1_df, mo):
    # max block for the slider
    max_block = commits_l1_df.select("mev_commit_block_number").max().item()

    max_block_slider = mo.ui.range_slider(
        start=0, stop=max_block, label="mev-commit block range"
    )
    max_block_slider
    return max_block, max_block_slider


@app.cell
def __(max_block_slider):
    max_block_slider.value
    return


@app.cell
def __(max_block_slider):
    print(min(max_block_slider.value))
    return


@app.cell
def __(max_block_slider):
    print(max(max_block_slider.value))
    return


@app.cell(hide_code=True)
def __(commits_l1_df, max_block_slider, mo, pl):
    # Cell 3 - display the transformed dataframe
    filtered_df = commits_l1_df.filter(
        pl.col("mev_commit_block_number") > min(max_block_slider.value)
    ).filter(pl.col("mev_commit_block_number") < max(max_block_slider.value))
    mo.ui.table(filtered_df)
    return filtered_df,


@app.cell
def __(mo):
    mo.md("""## BI Explorer""")
    return


@app.cell
def __(filtered_df, mo):
    mo.ui.data_explorer(filtered_df)
    return


@app.cell
def __():
    return


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()
