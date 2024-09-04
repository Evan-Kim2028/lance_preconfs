import asyncio
import polars as pl
from typing import Optional
import time
import logging

from lancedb_tables.lance_table import LanceTable
from mev_commit_sdk_py.hypersync_client import Hypersync

from mev_boost_py.proposer_payload import ProposerPayloadFetcher
from mev_boost_py.proposer_payload import Network

# Constants
MEV_BOOST_TABLE_NAME: str = "mev_boost_blocks"
INDEX: str = "block_number"
URI: str = "data"
SLEEP_INTERVAL: int = 60  # Time to wait between iterations (in seconds)
FETCH_TIMEOUT: int = 60  # Timeout for fetching data in seconds

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

holesky_client: Hypersync = Hypersync(url='https://holesky.hypersync.xyz')
lance_tables: LanceTable = LanceTable()

async def fetch_blocks() -> Optional[pl.DataFrame]:
    try:
        # get mev-boost data
        mev_boost_query: ProposerPayloadFetcher = ProposerPayloadFetcher(
            network=Network.HOLESKY,
        )
        mev_boost_blocks_df: pl.DataFrame = mev_boost_query.run().with_columns(pl.col('block_number').cast(pl.UInt64))

        # get latest 300 block numbers
        block_numbers: list[str] = (
            mev_boost_blocks_df.sort(by="block_number")["block_number"].unique().to_list()[-300:]
        )
        print(f'querying block range {min(block_numbers)} to {max(block_numbers)}')

        # get holesky block data
        holesky_blocks_df: pl.DataFrame = await holesky_client.get_blocks(from_block=min(block_numbers), to_block=max(block_numbers)+1)

        holesky_blocks_df = holesky_blocks_df.select('number', 'timestamp', 'hash', 'base_fee_per_gas', 'gas_used', 'extra_data')

        holesky_boost_blocks_df = (
            holesky_blocks_df
            .rename({'number': 'block_number'})
            .join(mev_boost_blocks_df, on='block_number', how='left', suffix='_mev_boost')
        )

        return holesky_boost_blocks_df
    except Exception as e:
        logger.error(f"Error fetching blocks data: {e}")
        return None

def write_data(data: pl.DataFrame, commitment_table_name: str, merge_on: str) -> None:
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
    holesky_boost_blocks_df: Optional[pl.DataFrame] = await fetch_blocks()
    if holesky_boost_blocks_df is not None:

        write_data(holesky_boost_blocks_df, MEV_BOOST_TABLE_NAME, merge_on=INDEX)
        logger.info("mev-boost-blocks updated")
    else:
        logger.warning("No data fetched to write.")

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logger.error(f"An error occurred in the main loop: {e}")
        time.sleep(SLEEP_INTERVAL)
