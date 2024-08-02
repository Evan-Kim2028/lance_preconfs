import dash
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
        (pl.col('dispatchTimestamp') - \
         pl.col('decayStartTimeStamp')).alias('bid_latency'),
        (pl.col('bid')/10**18).alias('bid_eth'),
        (pl.from_epoch('dispatchTimestamp',
         time_unit='ms')).dt.round('1s').alias('datetime')
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
    # 'dispatchTimestamp',
)

# Initialize the Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
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
]
)

# Define the callback to update the table based on the input value


@app.callback(
    Output('table', 'data'),
    [Input('bidder-input', 'value')]
)
def update_table(bidder_addr):
    if bidder_addr:
        # DF used to calculate bid latency
        df = (commit_df
              .filter(
                  pl.col('bidder') == bidder_addr.lower())
              )
        return df.to_dicts()
    return []


# Run the app
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)
