import asyncio
import polars as pl
import duckdb
from typing import Optional
import time

from lancedb_tables.lance_table import LanceTable
from mev_commit_sdk_py.hypersync_client import Hypersync

# Constants
COMMITMENT_TABLE_NAME: str = "commitments"
INDEX: str = "block_number"
URI: str = "data"
SLEEP_INTERVAL: int = 10  # Time to wait between iterations (in seconds)

# Initialize clients and LanceTable
client: Hypersync = Hypersync(url='https://mev-commit.hypersync.xyz')
lance_tables: LanceTable = LanceTable()


async def fetch_data(from_block: int) -> Optional[pl.DataFrame]:
    """
    Fetch data from hypersync client. Returns commitments_df dataframe or None if no data is fetched.
    Columns = 
    ```'block_number', 'blockNumber', 'txnHash', 'bid', 'commiter', 'bidder',
    'isSlash', 'decayStartTimeStamp', 'decayEndTimeStamp', 'dispatchTimestamp',
    'commitmentHash', 'commitmentIndex', 'commitmentDigest', 'commitmentSignature',
    'revertingTxHashes', 'bidHash', 'bidSignature', 'sharedSecretKey'```

    Args:
        from_block (int): The starting block number to fetch data from.

    Returns:
        Optional[pl.DataFrame]: The processed commitments data or None if no data is fetched.
    """
    commit_stores: pl.DataFrame = await client.get_commit_stores_v1(from_block=from_block, print_time=False)
    encrypted_stores: pl.DataFrame = await client.get_encrypted_commit_stores_v1(from_block=from_block, print_time=False)
    commits_processed: pl.DataFrame = await client.get_commits_processed_v1(from_block=from_block, print_time=False)

    # If any queries are empty, return None
    if commit_stores is None or encrypted_stores is None or commits_processed is None:
        return None

    # Join and process data
    commitments_df: pl.DataFrame = (
        encrypted_stores
        .join(commit_stores, on='commitmentIndex', how='inner')
        .with_columns(('0x' + pl.col("txnHash")).alias('txnHash'))
        .join(commits_processed.select('commitmentIndex', 'isSlash'), on='commitmentIndex', how='inner')
    ).select(
        'block_number', 'blockNumber', 'txnHash', 'bid', 'commiter', 'bidder',
        'isSlash', 'decayStartTimeStamp', 'decayEndTimeStamp', 'dispatchTimestamp',
        'commitmentHash', 'commitmentIndex', 'commitmentDigest', 'commitmentSignature',
        'revertingTxHashes', 'bidHash', 'bidSignature', 'sharedSecretKey'
    )

    return commitments_df


def get_latest_block() -> Optional[int]:
    """
    Get the latest block number from the LanceDB table using DuckDB.

    Returns:
        Optional[int]: The latest block number, or None if the table doesn't exist.
    """
    try:
        commitments_table = lance_tables.open_table(
            uri=URI, table=COMMITMENT_TABLE_NAME)
        con = duckdb.connect()
        commitments_table = commitments_table.to_lance()
        latest_block: int = con.sql(
            "SELECT MAX(block_number) FROM commitments_table").fetchall()[0][0]
        return latest_block
    except Exception:
        return None


async def write_data(data: pl.DataFrame, merge_on: str) -> None:
    """
    Write data to the LanceDB table.

    Args:
        data (pl.DataFrame): The data to write.
        merge_on (str): The column to merge on.
    """
    lance_tables.write_table(
        uri=URI, table=COMMITMENT_TABLE_NAME, data=data, merge_on=merge_on)


async def main() -> None:
    """
    Main function to check table existence and write data.
    """
    latest_block: Optional[int] = get_latest_block()
    # Start from 0 if the table doesn't exist.
    from_block: int = (latest_block+1) if latest_block is not None else 0

    print(f"Data fetched at {
        time.strftime('%Y-%m-%d %H:%M:%S')}")
    commitments_df: Optional[pl.DataFrame] = await fetch_data(from_block=from_block)
    if commitments_df is not None and not commitments_df.is_empty():
        print(f"New commitments: {commitments_df.shape[0]}")
        await write_data(commitments_df, merge_on=INDEX)
    else:
        print(f"Latest Block number: {latest_block} - No new data to write.")

if __name__ == "__main__":
    while True:
        asyncio.run(main())
        time.sleep(SLEEP_INTERVAL)
