import marimo

__generated_with = "0.8.5"
app = marimo.App(width="full", app_title="preconf_analytics")


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import polars as pl

    from lancedb_tables.lance_table import LanceTable
    from datetime import datetime, timedelta

    pl.Config.set_fmt_str_lengths(200)
    pl.Config.set_fmt_float("full")
    return LanceTable, datetime, mo, pd, pl, timedelta


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
        # todo - implement bid decay calculation
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
        .sort(by="datetime", descending=True)
    )
    return df,


@app.cell
def __(mo):
    mo.md("""# Preconf Analytics""")
    return


@app.cell
def __(mo):
    mo.md(r"""## Bidder Activity""")
    return


@app.cell
def __():
    import altair as alt
    return alt,


@app.cell
def __(df, pl):
    bidder_group_df = (
        df.group_by("bidder")
        .agg(
            pl.len().alias("bid_count"),
            pl.col("bid_eth").sum().alias("total_eth_bids"),
        )
        .sort(by="bid_count", descending=True)
    )
    return bidder_group_df,


@app.cell
def __(alt, bidder_group_df):
    # Bidder Activity Charts
    # Preconf Bid Count
    chart1 = (
        alt.Chart(bidder_group_df.head(10))
        .mark_bar()
        .encode(
            y=alt.Y("bidder:N", axis=None),  # Hide y-axis in the first chart
            x=alt.X(
                "bid_count:Q", title="Preconf Count", scale=alt.Scale(reverse=True)
            ),
        )
        .properties(width=600, height=300, title="Preconf Commitments")
    )

    # Total ETH Bid amount
    chart2 = (
        alt.Chart(bidder_group_df.head(10))
        .mark_bar()
        .encode(
            y=alt.Y(
                "bidder:N",
                axis=alt.Axis(
                    title="Bidders", titleAngle=0, titleY=-10, titleX=-80
                ),
            ),  # Center the y-axis title
            x=alt.X("total_eth_bids:Q", title="Total ETH Bids"),
        )
        .properties(
            width=600,  # Adjusted width for side-by-side display
            height=300,
            title="Total Bids (ETH)",
        )
    )

    # Combine the two charts side-by-side with better spacing
    combined_chart = alt.concat(chart1, chart2, spacing=0).resolve_scale(
        y="shared"  # Share the y-axis scale between the two charts
    )

    combined_chart.show()
    return chart1, chart2, combined_chart


@app.cell
def __(mo):
    mo.md("""## slash rate""")
    return


@app.cell
def __(df, pl):
    # Round the datetime column to the nearest hour
    date_truncate_df = df.with_columns(
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
    total_slash_rate = df_last_24_hours["slash_rate"].mean()


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
            color="white",  # Font color
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
    mo.md("""## Opened Commitments - Search for preconfirmation bids. Filter each column further by clicking on the column name.""")
    return


@app.cell
def __(df, mo):
    # max block for the slider
    max_block = df.select("mev_commit_block_number").max().item()

    max_block_slider = mo.ui.slider(
        start=0, stop=max_block, value=max_block, label="mev-commit block range"
    )
    max_block_slider
    return max_block, max_block_slider


@app.cell
def __(df, max_block_slider, mo, pl):
    # Cell 3 - display the transformed dataframe
    filtered_df = df.filter(pl.col("bid_decay_latency") < max_block_slider.value)
    mo.ui.table(filtered_df)
    return filtered_df,


if __name__ == "__main__":
    app.run()
