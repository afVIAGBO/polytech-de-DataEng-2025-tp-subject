import duckdb


def create_agregate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

#Dim_city
def agregate_dim_city():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql_statement)


def agregate_dim_station():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    sql_statement = """
        INSERT OR REPLACE INTO DIM_STATION
        SELECT 
            ID,
            CODE ,
            NAME ,
            ADDRESS ,
            LONGITUDE ,
            LATITUDE ,
            STATUS ,
            CAPACITTY 
        FROM CONSOLIDATE_STATION
        """

    con.execute(sql_statement)

##FACT_STATION_STATEMENT
def agregate_FACT_STATION_STATEMENT():
    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb", read_only=False)

    sql = """
        INSERT OR REPLACE INTO FACT_STATION_STATEMENT AS f
        SELECT
            css.STATION_ID,
            dc.ID AS CITY_ID,
            css.BICYCLE_DOCKS_AVAILABLE,
            css.BICYCLE_AVAILABLE,
            css.LAST_STATEMENT_DATE,
            css.CREATED_DATE
        FROM CONSOLIDATE_STATION_STATEMENT css
        JOIN CONSOLIDATE_STATION cs
            ON cs.ID = css.STATION_ID
        JOIN DIM_CITY dc
            ON dc.ID = cs.CITY_CODE
    """

    con.execute(sql)
