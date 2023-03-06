import argparse
from datetime import datetime
import io, os, requests
import pandas as pd

def extractReport(report_id, csv_name, report_columns):
    url = "https://api.bamboohr.com/api/gateway.php/foris/v1/reports/"+report_id+"?format=csv&onlyCurrent=false"

    headers = {"authorization": "Basic ZWY1MTY3YmZlY2VkMGVhZTQyNmMyOWFjZjkxMTk1NWM3YWE5NWI1Yzo="}

    response = requests.get(url, headers=headers, verify=False)
    df = pd.read_csv(io.StringIO(response.text))

    # rename columns
    if(report_columns):
        df.columns = report_columns

    # set columns as text
    df.astype(str)

    # save as csv file
    if(len(df.index)>0):
        datetime.today().strftime("%Y%m%d")
        df.to_csv('csv/'+csv_name
            ,index=False
            )
    return;


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Downloading a bamboo report into a csv file.',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-rid", help="a bamboo report id", required=False)
    parser.add_argument("-csv", help="a source csv file", required=False)
    args = vars(parser.parse_args())
    rid = args["rid"]
    csv = args["csv"]

    extractReport(rid,csv,[])
    