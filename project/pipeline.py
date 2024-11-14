import os
import shutil
import pandas as pd
import sqlite3
import requests
from io import BytesIO

class DataExtractor:
    def __init__(self, url):
        self.url = url

    def fetch_csv(self):
        # Fetch the CSV data into a DataFrame
        df = pd.read_csv(self.url)
        return df
    def fetch_xls(self):
        # Fetch the XLS data into a DataFrame
        df = pd.read_excel(self.url)
        return df

class DataTransformer:
    def __init__(self, selected_columns, drop_zero=None, rename_columns=None, fahrenheit_column=None, celsius_column=None):
        self.selected_columns = selected_columns
        self.drop_zero = drop_zero
        self.rename_columns_map = rename_columns

    def select_columns(self, df):
        # Select specific columns
        return df[self.selected_columns]

    def drop_zero_values(self, df):
        # Drop rows where the specified column value is 0.0, if applicable
        if self.drop_zero:
            df = df[df[self.drop_zero] != 0.0]
        return df

    def rename_columns(self, df):
        # Rename columns, if applicable
        if self.rename_columns_map:
            df = df.rename(columns=self.rename_columns_map)
        return df
    
    def perform_join(self, df1, df2, join_columns):
        # Perform a join on specified columns
        return pd.merge(df1, df2, on=join_columns)

    def remove_null(self, df):
        # Removes rows with null values. 
        return df.dropna()

 

    def process_data(self, df):
        df = self.select_columns(df)
        df = self.remove_null(df)
        df = self.drop_zero_values(df)
        df = self.rename_columns(df)
        df = self.convert_fahrenheit(df)
        return df

class DataLoader:
    def __init__(self, folder_path, sqlite_filename):
        self.folder_path = folder_path
        self.sqlite_filename = sqlite_filename

    def create_directory(self):
        # Ensures the target directory exists or creates it
        shutil.rmtree(self.folder_path, ignore_errors=True)
        os.makedirs(self.folder_path)

    def save_to_sqlite(self, df, table_name):
        # Save the DataFrame to SQLite database
        conn = sqlite3.connect(os.path.join(self.folder_path, self.sqlite_filename))
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()

class Application:
    def __init__(self, extractor, transformer, loader=None):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        df = self.extractor.fetch_csv()
        processed_df = self.transformer.process_data(df)
        return processed_df
    def run(self):
        df = self.extractor.fetch_xls()
        processed_df = self.transformer.process_data(df)
        return processed_df

# Dataset 1: Total energy consumption in US(renewable and non-renewable)
total_url = "https://data.openei.org/files/276/usmonthlyaveragehourlyemissionfactorsforaznmegridsubregion.csv"
total_selected_columns = ['Month', 'Total Fossil Fuels Consumption', 'Total Renewable Energy Consumption']
total_extractor = DataExtractor(total_url)
total_transformer = DataTransformer(total_selected_columns)
total_app = Application(total_extractor, total_transformer)
total_df_selected = total_app.run()  # Running the extractor and transformer

# Dataset 2: Daily emission in US
emisson_url = "https://data.openei.org/files/276/usmonthlyaveragehourlyemissionfactorsforaznmegridsubregion.csv"
emisson_selected_columns = df.iloc[:, [3,7,11]]
emisson_rename_columns = list(df.columns)
columns[3] = 'CO2 Average'  # Rename column at position 3
columns[7] = 'SO2 Average'   # Rename column at position 7
columns[11] = 'NOx Aveage'  # Rename column at position 11

# Assign the updated column names back to the DataFrame
df.columns = columns

emisson_extractor = DataExtractor(temperature_url)
emisson_transformer = DataTransformer(
    emisson_selected_columns, 
    rename_columns=emisson_url_rename_columns,
)
emisson_app = Application(emisson_extractor, emisson_transformer)
emisson_df_selected = emisson_app.run()  # Running the extractor and transformer

# Perform a join
data_transformer = DataTransformer([], None)
joined_df = data_transformer.perform_join(total_df_selected, energy_df_selected)

# Save the joined DataFrame to SQLite database
folder_path = "../data"
sqlite_filename = "energy.sqlite"
data_loader = DataLoader(folder_path, sqlite_filename)
data_loader.create_directory()
data_loader.save_to_sqlite(joined_df, "energy")
