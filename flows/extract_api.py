import os
import json
import requests
import pandas as pd
from prefect import task, flow

@task()
def get_asset_data(url: str, csv_file_path: str) -> str:
    """Get asset data from CoinCap API and save it as a CSV file."""
    
    response = requests.get(url)
    
    if response.status_code == 200:
        
        json_data = response.json()
        data = json_data["data"]

        df = pd.DataFrame(data)
        
        if not os.path.exists(os.path.dirname(csv_file_path)):
            os.makedirs(os.path.dirname(csv_file_path))
        
        # Save the DataFrame as a CSV file in the specified path
        df.to_csv(csv_file_path, index=False)
        
        # Return the CSV file path
        return csv_file_path
    else:
        # Handle the case when the request fails
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return None
    
@task()
def transform_asset(csv_file_path: str) -> pd.DataFrame:
    """Transform raw asset data into cleaned format for further analysis."""
    # Load data from the csv file into a dataframe
    df = pd.read_csv(csv_file_path)
    
    # Clean the data: Replace None values with appropriate defaults (e.g., 0)
    df.fillna(0, inplace=True)

    # Convert numeric columns to appropriate data types
    numeric_columns = ["supply", "maxSupply", "marketCapUsd", "volumeUsd24Hr", "priceUsd", "changePercent24Hr", "vwap24Hr"]

    df[numeric_columns] = df[numeric_columns].astype(float)

    df = df.drop('explorer', axis=1)

    print(df)
    return df
    

@flow()
def Extract_Load_transform() -> None:
    # Define the URL to fetch data from
    url = "http://api.coincap.io/v2/assets"
    
    csv_file_path = "./asset_data/asset-data.csv"
    
    df = get_asset_data(url, csv_file_path)
    df_csv = transform_asset(df)

if __name__ == "__main__":
    Extract_Load_transform()