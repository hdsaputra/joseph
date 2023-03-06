#Identify the last time database is updated
import argparse
import psycopg2

def loadFinalTable (transformer):
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port="2000",
            database="hrta",
            user="app_hrta",
            password="Vi0Tm1G4OgzslmIyESL9"
        )
        dwh = conn.cursor()

        dwh.execute("CALL internal."+transformer)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print(error)
    finally:
        if conn:
            dwh.close()
            conn.close()
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Calling Stored Procedure to load Fact Table from Staging Table',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-sp", help="a stored procedure name", required=False)
    args = vars(parser.parse_args())
    sp = args["sp"]

    loadFinalTable(sp)
