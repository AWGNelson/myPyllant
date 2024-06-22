import argparse
import asyncio
import logging
import csv
import os
from datetime import datetime, timedelta, timezone

from src.myPyllant.api import MyPyllantAPI
from src.myPyllant.const import ALL_COUNTRIES, BRANDS, DEFAULT_BRAND
from src.myPyllant import export

parser = argparse.ArgumentParser(description="Export data from myVaillant API   .")
parser.add_argument("user", help="Username (email address) for the myVaillant app")
parser.add_argument("password", help="Password for the myVaillant app")
parser.add_argument(
    "brand",
    help="Brand your account is registered in, i.e. 'vaillant'",
    default=DEFAULT_BRAND,
    choices=BRANDS.keys(),
)
parser.add_argument(
    "--country",
    help="Country your account is registered in, i.e. 'germany'",
    choices=ALL_COUNTRIES.keys(),
    required=False,
)
parser.add_argument(
    "-v", "--verbose", help="increase output verbosity", action="store_true"
)

async def main(user, password, brand, country):
    async with MyPyllantAPI(user, password, brand, country) as api:


        # Function to write data to CSV
        def write_to_csv(system):
            # Define the new data to be appended
            system.primary_heat_generator.current_power
            new_data = {'Time': datetime.now(), **system.state['system'], **system.state['zones'][0], **system.state['circuits'][0], **system.state['dhw'][0]}

            # Define the column names (these should match the existing CSV structure)
            fieldnames = list(new_data.keys())

            # Check if the file exists
            file_exists = os.path.isfile('output.csv')

            # Open the file in append mode to add the new data
            with open('output.csv', mode='a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)

                # Write the field names as the first row if the file doesn't exist
                if not file_exists:
                    writer.writeheader()
                
                # Append the new row of data
                writer.writerow(new_data)

        # Function to schedule the writing task every minute
        async def schedule_task(api):
            while True:
                try:
                    # Calculate the delay until the next full minute
                    now = datetime.now()
                    next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                    delay = (next_run - now).total_seconds()

                    # Wait until the next full minute
                    await asyncio.sleep(delay)

                    print(api.oauth_session_expires)
                    dt = datetime.now(timezone.utc).astimezone(timezone.utc)
                    if (
                        api.oauth_session_expires is None
                        or api.oauth_session_expires
                        < dt + timedelta(seconds=180)
                    ):
                        logging.debug("Refreshing token for %s", api.username)
                        logging
                        await api.refresh_token()

                    async for system in api.get_systems():
                        # Call the write_to_csv function
                        write_to_csv(system)
                except:
                    Warning("Unable to fetch data")


        # Start the initial scheduling
        await schedule_task(api)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    a = asyncio.run(main("peter@petereastern.com", "!d9kmypCW8Nd@KvGE3PJt", "vaillant", "unitedkingdom"))
    print(a)