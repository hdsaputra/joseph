#Identify the last time database is updated
import argparse
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def insert_to_staging (csv_file, stg_table):
    conn = psycopg2.connect(
        host="127.0.0.1",
        port="2000",
        database="hrta",
        user="app_hrta",
        password="Vi0Tm1G4OgzslmIyESL9"
    )
    dwh = conn.cursor()

    dwh.execute("TRUNCATE TABLE stg."+stg_table)
    conn.commit()

    conn_str = 'postgresql://app_hrta:Vi0Tm1G4OgzslmIyESL9@127.0.0.1:2000/hrta'
    db = create_engine(conn_str)

    df = pd.read_csv('csv/'+ csv_file)
    df.astype(str)
    df.to_sql(stg_table, db, schema='stg', if_exists='append', index=False)

    return 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Inserting a single csv file into a staging database table.',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-csv", help="a source csv file", required=False)
    parser.add_argument("-stg", help="a destination staging table", required=False)
    args = vars(parser.parse_args())
    csv = args["csv"]
    stg = args["stg"]
    insert_to_staging(csv, stg)
