import dash
import plotly.express as px
import polars as pl

from dash import dcc, html, dash_table, Input, Output
from lancedb_tables.lance_table import LanceTable

# Constants
COMMITMENT_TABLE_NAME: str = "commitments"
INDEX: str = "block_number"
URI: str = "data"

# Initialize clients and LanceTable
lance_tables: LanceTable = LanceTable()
commitments_table = lance_tables.open_table(
    uri=URI, table=COMMITMENT_TABLE_NAME)

commit_df = (
    pl.from_arrow(commitments_table.to_lance().to_table())
    # calculate bid latency
    .with_columns(
        (pl.from_epoch('dispatchTimestamp',
                       time_unit='ms')).dt.round('1s').alias('datetime'),
        (pl.col('dispatchTimestamp') -
         pl.col('decayStartTimeStamp')).alias('bid_latency'),
        (pl.col('bid')/10**18).alias('bid_eth'),
        (pl.col('decayEndTimeStamp') -
         pl.col('decayStartTimeStamp')).alias('total_decay_range'),
        # if decay_time_past is negative, it means that the provider commited outside of the decay range so the bid would have fully decayed to 0.
        (pl.col('decayEndTimeStamp') -
         pl.col('dispatchTimestamp')).alias('decay_time_past'),
    )
    .with_columns(
        # total decay time is the range [beginDecayTimestamp, endDecayTimestamp] and `decay_time_past` is the time the provider commited to the transaction.
        (1 - (pl.col('decay_time_past') / pl.col('total_decay_range'))
         ).alias('decay_percent')
    )
    .with_columns(
        (pl.col('bid') * pl.col('decay_percent') / 10**18).alias('decayed_bid_eth'),
    )
    .sort(by='datetime', descending=True)
).rename({
    'blockNumber': 'holesky_block_number'
}).select(
    'datetime',
    'holesky_block_number',
    'bidder',
    'commiter',
    'bid_eth',
    'bid_latency',
    'txnHash',
    'isSlash',
    'decayed_bid_eth',
    # 'dispatchTimestamp',
)

# undecayed bid vol
total_bids = commit_df.with_columns(pl.col('datetime').dt.round('1d')).group_by('datetime').agg(
    pl.sum('bid_eth').alias('total_bids'),
)

# decayed bid vol
total_decayed_bids = commit_df.filter(pl.col('decayed_bid_eth') > 0).with_columns(pl.col('datetime').dt.round('1d')).group_by('datetime').agg(
    pl.sum('decayed_bid_eth').alias('total_decayed_bids_eth'),
)

bid_volume_df = total_bids.join(total_decayed_bids, on='datetime', how='inner')

# Convert to Pandas DataFrame for Plotly
bid_volume_df_pd = bid_volume_df.to_pandas()


# Create the bar chart
fig = px.bar(
    bid_volume_df_pd,
    x='datetime',
    y=['total_bids', 'total_decayed_bids_eth'],
    barmode='group',
    labels={'datetime': 'DateTime', 'value': 'ETH'},
    title='Total Bid and Decayed Bid Volume'
)

# Initialize the Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
    dcc.Graph(figure=fig),
    html.H6("Bidder Latency: Enter bidder address below"),
    dcc.Input(
        id="bidder-input",
        type="text",
        placeholder="Enter bidder address"
    ),
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in commit_df.columns]
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
