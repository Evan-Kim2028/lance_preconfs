import asyncio
import polars as pl

from lancedb_tables.lance_table import LanceTable
from mev_commit_sdk_py.hypersync_client import Hypersync


# query window data
client = Hypersync(url='https://mev-commit.hypersync.xyz')


async def get_data():
    # get latest block height
    block_height = await client.get_height()
    from_block = block_height - 8000000

    commit_stores: pl.DataFrame = asyncio.run(
        client.get_commit_stores_v1(from_block=from_block))
    encrypted_stores: pl.DataFrame = asyncio.run(
        client.get_encrypted_commit_stores_v1(from_block=from_block))

    # get commitment slashing
    commits_processed = asyncio.run(
        client.get_commits_processed_v1(from_block=from_block))

    # join encrypted_stores to commit_stores. This narrows down the bids to the ones that were opened and committed to on chain.
    commitments_df = (
        encrypted_stores
        .join(
            commit_stores, on='commitmentIndex', how='inner').with_columns(
            ('0x' + pl.col("txnHash")).alias('txnHash'))
        .join(
            commits_processed.select('commitmentIndex', 'isSlash'), on='commitmentIndex', how='inner')
    ).select('blockNumber',    # holesky block number
             'txnHash',        # holesky tx hash
             'datetime',
             'bid',
             'decayed_bid_eth',
             'commiter',
             'bidder',
             'isSlash',
             'bid_latency',
             'decayStartTimeStamp',
             'decayEndTimeStamp',
             'dispatchTimestamp',
             'total_decay_range',
             'decay_time_past',
             'decay_percent'
             )

    # Write blocks and transactions data into lancedb tables.
    commitment_table_name = "commitments"
    index: str = "blockNumber"

    # pull data into lance table
    lance_tables = LanceTable()

    # write blocks
    lance_tables.write_table(uri="data", table=commitment_table_name, data=commitments_df, merge_on=index
                             )
