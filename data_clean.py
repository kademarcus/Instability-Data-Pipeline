''' 
- Idea for this project is to make a large dataset describing global poltiical Instability
- Will pull from sources such as UCDP, World Bank, and the Fragile States Index
- Will have large dataset of political instability variables such as protests, regime type, conflict, etc.
- Will store in a SQL data warehouse and have datareporting from PowerBI
- This python scripts main purpose is to import, ingest, and clean data from multiple sources
'''
#install world bank package
#!pip install wbdata

#import data management packages
import pandas as pd
import requests
import os
import datetime as dt
import pandas_datareader as pdr
import wbdata #worldbank data


#Conflict Data
def UCDP_load():
  url = "https://ucdp.uu.se/downloads/ged/ged251-csv.zip"
  df = pd.read_csv(url, compression = "zip")
 
  #list variables of interest
  variables = [
        "conflict_id",          # Unique conflict identifier
        "year",                 # Year
        "location",              # Location 
        "conflict_name",        # Name of conflict
        "conflict_type",        # Type of conflict (e.g., interstate, intrastate)
        "best",                 # Conflict intensity (coded 1-3; 1=low intensity, 3=war)
        "start_year",           # Start year of conflict
        "end_year",             # End year of conflict (if ended)
        "side_a",               # Party A name
        "side_b",               # Party B name
        "intensity_level"       #intesity of conflict
    ]
  
  #some data manipulation
  cols_to_use = [col for col in variables if col in df.columns] #only want to keep variables in list
  df = df[cols_to_use] #see above

  
  df["year"] = df["year"].astype(int) #make sure year is integer type for easier merge
  df["conflict_present"] = 1 #create variable for conflict being present (important for final dataset)

  agg_df = df.groupby(["country", "year"]).agg({     #aggregate dataset (countries with multiple entries for one year)
        "conflict_present": "max",      # 1 if any conflict present
        "best": "max"                   # max conflict intensity
    }).reset_index()

    
  return agg_df

 

#Econ Data
def worldbank_load():
    indicators = {
        "PV.EST": "political_stability",
        'NY.GDP.PCAP.CD': 'gdp_per_capita',
        "SP.POP.TOTL": 'population',
        "SE.XPD.TOTL.GD.ZS": 'education_spending'
    }

    # Download raw data (no keyword args)
    df = wbdata.get_dataframe(indicators)

    # Reset index (country, date are in index)
    df = df.reset_index()

    # Extract year from datetime index (it’s stored as datetime64[ns] or object)
    df['year'] = pd.to_datetime(df['date']).dt.year

    # Filter years
    df = df[df['year'].between(1946, 2024)]

    # country as string
    df['country'] = df['country'].astype(str)

    #year as integer as filtering complete
    df['year'] = df['year'].astype(int)

    # Select final columns
    df = df[['country',
             'year', 
             'political_stability', 
             'gdp_per_capita', 
             'population', 
             'education_spending']]

    return df



def main():
  ucdp = UCDP_load()
  worldbank = worldbank_load()
  print(worldbank.head())
  print("Data Split Here----------------")
  print(ucdp.head())

if __name__ == "__main__":
  main()

