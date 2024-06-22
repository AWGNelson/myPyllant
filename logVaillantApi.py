import argparse
import asyncio
import logging
import csv
import os
from datetime import datetime, timedelta, timezone
import pandas as pd

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

async def main(brand, country):
    async def logVaillantData():
        # load db with connected Vaillants
        # Read the CSV file into a DataFrame
        df = pd.read_csv(r'G:\My Drive\HavenWise\Database\myVaillant.csv')

        # for each Vaillant, get and save updated data to db
        # Iterate through each row
        for index, row in df.iterrows():
            # Access row values using row['column_name'] or row[index]          

            async with MyPyllantAPI(row.login, row.password, brand, country) as api:
                async for system in api.get_systems(include_connection_status=True,include_diagnostic_trouble_codes=True,include_rts=True,include_mpc=True):
                    # Function to write data to CSV
                    def write_to_csv(system, rowId):
                        # Define the new data to be appended
                        new_data = {'Time': datetime.now(), **system.state['system'], **system.state['zones'][0], **system.state['circuits'][0], **system.state['dhw'][0],
                                    'current_power':system.mpc['devices'][0]['current_power'], **system.primary_heat_generator.rts_statistics,
                                    'device_serial_number':system.primary_heat_generator.device_serial_number,**system.configuration['circuits'][0],**system.configuration['zones'][0],
                                    **system.configuration['dhw'][0], **system.configuration['system'],'is_cylinder_boosting':system.domestic_hot_water[0].is_cylinder_boosting}

                        # Define the column names (these should match the existing CSV structure)
                        fieldnames = list(new_data.keys())

                        directory=r'G:\My Drive\HavenWise\Database\myVaillantData'

                        if not os.path.isdir(directory):
                            os.makedirs(directory)

                        saveFilePath = os.path.join(directory, rowId + ".csv")

                        # Check if the file exists
                        file_exists = os.path.isfile(saveFilePath)

                        # Open the file in append mode to add the new data
                        with open(saveFilePath, mode='a', newline='') as file:
                            writer = csv.DictWriter(file, fieldnames=fieldnames)

                            # Write the field names as the first row if the file doesn't exist
                            if not file_exists:
                                writer.writeheader()
                            
                            # Append the new row of data
                            writer.writerow(new_data)

                    # Call the write_to_csv function
                    write_to_csv(system, row.id)

    # Function to schedule the writing task every minute
    async def schedule_task():
        while True:
            try:
                # Calculate the delay until the next full minute
                now = datetime.now()
                next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                delay = (next_run - now).total_seconds()

                # Wait until the next full minute
                await asyncio.sleep(delay)

                #print(api.oauth_session_expires)
                #dt = datetime.now(timezone.utc).astimezone(timezone.utc)
                # if (
                #     api.oauth_session_expires is None
                #     or api.oauth_session_expires
                #     < dt + timedelta(seconds=180)
                # ):
                #     logging.debug("Refreshing token for %s", api.username)
                #     await api.refresh_token()

                # async for system in api.get_systems():
                #     # Call the write_to_csv function
                #     write_to_csv(system, api)
                await logVaillantData()

            except:
                Warning("Unable to fetch data")

    # Start the initial scheduling
    await schedule_task()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main("vaillant", "unitedkingdom"))