import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_city_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/communes.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data, max_level=1)

    cols = ["code", "nom", "population"]
    existing_cols = [c for c in cols if c in raw_data_df.columns]
    city_data_df = raw_data_df[existing_cols].copy()
    city_data_df.rename(columns={
        "code": "ID",
        "nom": "NAME",
        "population": "NB_INHABITANTS"
    }, inplace=True)

    city_data_df["CREATED_DATE"] = date.today()

    city_data_df.drop_duplicates(inplace = True)

    print(city_data_df)  
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")


# CONSOLIDATE_STATION

def CONSOLIDATE_STATION_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Lecture du JSON
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    # Sélection des colonnes
    station_df = raw_data_df[[
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "capacity",
        "duedate"
    ]]

    
    # Création de l’ID
    station_df["id"] = station_df["stationcode"].astype(str) + "_" + station_df["duedate"].astype(str)
    station_df["ADDRESS"] = None

    # Renommage
    station_df.rename(columns={
        "id": "ID",
        "stationcode": "CODE",
        "name": "NAME",
        "nom_arrondissement_communes": "CITY_NAME",
        "code_insee_commune": "CITY_CODE",
        "coordonnees_geo.lon": "LONGITUDE",
        "coordonnees_geo.lat": "LATITUDE",
        "is_installed": "STATUS",
        "capacity": "CAPACITTY",
        "duedate": "CREATED_DATE"
    }, inplace=True)

    # On met les colonnes EXACTEMENT dans l’ordre de la table :
    station_df = station_df[
        [
            "ID" ,
            "CODE", 
            "NAME" ,
            "CITY_NAME" ,
            "CITY_CODE" ,
            "ADDRESS" ,
            "LONGITUDE" ,
            "LATITUDE" ,
            "STATUS" ,
            "CREATED_DATE" ,
            "CAPACITTY" 
        ]
    ]

    # Drop des doublons
    station_df.drop_duplicates(inplace=True)

    # Insertion dans DuckDB
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_df")


#CONSOLIDATE_STATION_STATEMENT

def CONSOLIDATE_STATION_Statement_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data, max_level=1)

    # Sélection et renommage des colonnes
    station_state_df = raw_data_df[[
        "stationcode",
        "numdocksavailable",
        "numbikesavailable", 
        "duedate"
    ]]

    #génération d'ID
    station_state_df["LAST_STATEMENT_DATE"] = today_date
    station_state_df["STATION_ID"] = station_state_df["stationcode"].astype(str) + "_" + station_state_df["duedate"].astype(str)

    station_state_df.rename(columns={
        "numdocksavailable": "BICYCLE_DOCKS_AVAILABLE",
        "numbikesavailable": "BICYCLE_AVAILABLE",
        "duedate": "CREATED_DATE"

    }, inplace=True)

    station_state_df = station_state_df[["STATION_ID",
                                        "BICYCLE_DOCKS_AVAILABLE" ,
                                        "BICYCLE_AVAILABLE" ,
                                        "LAST_STATEMENT_DATE" ,
                                        "CREATED_DATE"]]
    station_state_df.drop_duplicates(inplace = True)
    
    print(station_state_df)
    
    # Insertion dans DuckDB
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_state_df")


############### Nantes ###############

def CONSOLIDATE_STATION_data_NANTES():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Charger JSON Nantes
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data["results"])

    # Colonnes obligatoires pour ta table
    station_df = pd.DataFrame()
    station_df["ID"] = raw_data_df["number"].astype(str) + "_" + raw_data_df["last_update"].astype(str)
    station_df["CODE"] = raw_data_df["number"].astype(str)
    station_df["NAME"] = raw_data_df["name"]
    station_df["CITY_NAME"] = raw_data_df["contract_name"]
    station_df["CITY_CODE"] = "44109"
    station_df["ADDRESS"] = raw_data_df.get("address", None)
    station_df["LONGITUDE"] = raw_data_df["position.lon"]
    station_df["LATITUDE"] = raw_data_df["position.lat"]
    station_df["STATUS"] = raw_data_df["status"]
    station_df["CREATED_DATE"] = raw_data_df["last_update"]
    station_df["CAPACITTY"] = raw_data_df["bike_stands"]

    # Ordre exact des colonnes DuckDB
    station_df = station_df[
        [
            "ID",
            "CODE",
            "NAME",
            "CITY_NAME",
            "CITY_CODE",
            "ADDRESS",
            "LONGITUDE",
            "LATITUDE",
            "STATUS",
            "CREATED_DATE",
            "CAPACITTY"
        ]
    ]

    station_df.drop_duplicates(inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_df")



def CONSOLIDATE_STATION_Statement_data_NANTES():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_df = pd.json_normalize(data["results"])

    station_state_df = pd.DataFrame()
    
    station_state_df["STATION_ID"] = raw_df["number"].astype(str) + "_" + raw_df["last_update"].astype(str)
    station_state_df["BICYCLE_DOCKS_AVAILABLE"] = raw_df["available_bike_stands"]
    station_state_df["BICYCLE_AVAILABLE"] = raw_df["available_bikes"]
    station_state_df["LAST_STATEMENT_DATE"] = date.today()
    station_state_df["CREATED_DATE"] = raw_df["last_update"]

    station_state_df = station_state_df[
        [
            "STATION_ID",
            "BICYCLE_DOCKS_AVAILABLE",
            "BICYCLE_AVAILABLE",
            "LAST_STATEMENT_DATE",
            "CREATED_DATE"
        ]
    ]

    station_state_df.drop_duplicates(inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_state_df")

