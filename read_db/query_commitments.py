import asyncio
import polars as pl
import duckdb
from typing import Optional, Union
import time
import logging

from lancedb_tables.lance_table import LanceTable
from mev_commit_sdk_py.hypersync_client import Hypersync

# Constants
COMMITMENT_TABLE_NAME: str = "commitments"
L1_TX_TABLE_NAME: str = "l1_txs"
INDEX: str = "block_number"
URI: str = "data"
SLEEP_INTERVAL: int = 25  # Time to wait between iterations (in seconds)
FETCH_TIMEOUT: int = 30  # Timeout for fetching data in seconds

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize clients and LanceTable
mev_commit_client: Hypersync = Hypersync(url='https://mev-commit.hypersync.xyz')
holesky_client: Hypersync = Hypersync(url='https://holesky.hypersync.xyz')
lance_tables: LanceTable = LanceTable()

async def fetch_opened_commits(from_block: int) -> Optional[pl.DataFrame]:
    """
    Fetch data from hypersync client. Returns commitments_df dataframe or None if no data is fetched.
    """
    try:
        commit_stores = await asyncio.wait_for(
            mev_commit_client.execute_event_query('OpenedCommitmentStored', from_block=from_block), 
            FETCH_TIMEOUT
        )
        encrypted_stores = await asyncio.wait_for(
            mev_commit_client.execute_event_query('UnopenedCommitmentStored', from_block=from_block), 
            FETCH_TIMEOUT
        )
        commits_processed = await asyncio.wait_for(
            mev_commit_client.execute_event_query('CommitmentProcessed', from_block=from_block), 
            FETCH_TIMEOUT
        )

        if commit_stores is None or encrypted_stores is None or commits_processed is None:
            logger.warning("One or more event queries returned None.")
            return None

        commitments_df = (
            encrypted_stores
            .join(commit_stores, on='commitmentIndex', how='inner')
            .with_columns(('0x' + pl.col("txnHash")).alias('txnHash'))
            .join(commits_processed.select('commitmentIndex', 'isSlash'), on='commitmentIndex', how='inner')
        ).select(
            'block_number', 'timestamp', 'blockNumber', 'txnHash', 'bid', 'commiter', 'bidder',
            'isSlash', 'decayStartTimeStamp', 'decayEndTimeStamp', 'dispatchTimestamp',
            'commitmentHash', 'commitmentIndex', 'commitmentDigest', 'commitmentSignature',
            'revertingTxHashes', 'bidHash', 'bidSignature', 'sharedSecretKey'
        )

        return commitments_df

    except asyncio.TimeoutError as e:
        logger.error(f"Timeout while fetching opened commits data: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while fetching opened commits: {e}")
        return None

async def fetch_l1_txs(l1_tx_list: Union[str, list[str]]) -> Optional[pl.DataFrame]:
    """
    Fetch l1 tx data from hypersync client. Returns l1_txs_df dataframe or None if no data is fetched.
    """
    try:
        if not l1_tx_list:  # Check if list is empty
            logger.info("No L1 transaction hashes to query.")
            return None

        l1_txs = await asyncio.wait_for(
            holesky_client.search_txs(txs=l1_tx_list), 
            FETCH_TIMEOUT
        )

        if l1_txs is None or l1_txs.is_empty():
            logger.info("No L1 transactions found.")
            return None

        return l1_txs

    except asyncio.TimeoutError as e:
        logger.error(f"Timeout while fetching L1 transactions: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while fetching L1 transactions: {e}")
        return None

def get_latest_block(commitment_table_name: str) -> Optional[int]:
    """
    Get the latest mev-commit block number from the LanceDB table using DuckDB.
    """
    try:
        commitments_table = lance_tables.open_table(uri=URI, table=commitment_table_name)
        con = duckdb.connect()
        commitments_table = commitments_table.to_lance()
        latest_block: int = con.sql("SELECT MAX(block_number) FROM commitments_table").fetchall()[0][0]
        return latest_block
    except Exception as e:
        logger.error(f"Error getting latest block number: {e}")
        return None

async def write_data(data: pl.DataFrame, commitment_table_name: str, merge_on: str) -> None:
    """
    Write data to the LanceDB table.
    """
    try:
        lance_tables.write_table(uri=URI, table=commitment_table_name, data=data, merge_on=merge_on)
    except Exception as e:
        logger.error(f"Error writing data to LanceDB: {e}")

async def main() -> None:
    """
    Main function to get commitments and L1 transactions data.
    """
    latest_block: Optional[int] = get_latest_block(commitment_table_name=COMMITMENT_TABLE_NAME)
    logger.info(f'Latest block: {latest_block}')
    
    from_block: int = (latest_block + 1) if latest_block is not None else 0

    logger.info(f"Fetching data starting from block: {from_block} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    commitments_df: Optional[pl.DataFrame] = await fetch_opened_commits(from_block=from_block)

    if commitments_df is not None and not commitments_df.is_empty():
        l1_txs_list = commitments_df.select("txnHash").unique()["txnHash"].to_list()

        if l1_txs_list:  # Check if there are any transaction hashes to query
            logger.info(f"Fetching L1 transactions for {len(l1_txs_list)} hashes.")
            l1_txs_df: Optional[pl.DataFrame] = await fetch_l1_txs(l1_tx_list=l1_txs_list)
        else:
            l1_txs_df = None

        await write_data(commitments_df, COMMITMENT_TABLE_NAME, merge_on=INDEX)
        logger.info(f"New commitments written: {commitments_df.shape[0]}")

        if l1_txs_df is not None and not l1_txs_df.is_empty():
            await write_data(l1_txs_df, L1_TX_TABLE_NAME, merge_on=INDEX)
            logger.info(f"New L1 transactions written: {l1_txs_df.shape[0]}")
    else:
        logger.info("No new commitments data to write.")

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logger.error(f"An error occurred in the main loop: {e}")
        time.sleep(SLEEP_INTERVAL)
