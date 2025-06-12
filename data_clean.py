''' 
- Idea for this project is to make a large dataset describing global poltiical Instability
- Will pull from sources such as UCDP, World Bank, and the Fragile States Index
- Will have large dataset of political instability variables such as protests, regime type, conflict, etc.
- Will store in a SQL data warehouse and have datareporting from PowerBI
- This python scripts main purpose is to import, ingest, and clean data from multiple sources
'''

#import data management libraries
import pandas as pd
import requests
import os
from datetime import datetime as dt

#Conflict Data
def UCDP_load():
  url = "https://ucdp.uu.se/downloads/ged/ged251-csv.zip"
  df = pd.read_csv(url, compression = "zip")
 
  #list variables of interest
  variables = [
        "conflict_id",          # Unique conflict identifier
        "year",                 # Year
        "country",              # Country name
        "conflict_name",        # Name of conflict
        "conflict_type",        # Type of conflict (e.g., interstate, intrastate)
        "best",                 # Conflict intensity (coded 1-3; 1=low intensity, 3=war)
        "start_year",           # Start year of conflict
        "end_year",             # End year of conflict (if ended)
        "side_a",               # Party A name
        "side_b",               # Party B name
        "fatalities"            # Number of fatalities (if available)
    ]
  
  #some data manipulation
  cols_to_use = [col for col in variables if col in df.columns] #only want to keep variables in list
  df = df[cols_to_use] #see above
  df["year"] = df["year"].astype(int) #make sure year is integer type for easier merge
  df["conflict_present"] = 1 #create variable for conflict being present (important for final dataset)

  agg_df = df.groupby(["country", "year"]).agg({     #aggregate dataset (countries with multiple entries for one year)
        "conflict_present": "max",      # 1 if any conflict present
        "fatalities": "sum",            # total fatalities in that year-country
        "best": "max"                   # max conflict intensity
    }).reset_index()

    # Fill missing fatalities with zero
  agg_df["fatalities"] = agg_df["fatalities"].fillna(0)

  return agg_df

  pass

#Econ Data
def worldbank_load():
  pass

#Fragile States Data
def fragiles_load():
  pass

def main():
  ucdp = UCDP_load()
  worldbank = worldbank_load()
  fragiles = fragiles_load()

if __name__ == "__main__":
  main()
