from Extract import *
from Load import *
from Transform import *

master = {
    'reportKey':['bamboohire','bambooexit','bambooccchange','ihrcc'],
    'dataSource':['bambooHR','bambooHR','bambooHR','Gsheet'],
    'reportId': ['1111', '1276','888','x'],
    'columns' : [['empnum','workemail','lastname','firstname','preferredname','startdate','team','location','payratecurrency', 'payrate','payschedule']
                ,['empnum','exitdate','exittype','exitreason']
                ,['empnum','costcenterlv2','effectivedate']
                ,['empnum','costcenterlv2','effectivedate']
                ],
    'csvFile': ['bamboo_hire.csv','bamboo_exit.csv','bamboo_cc_chg.csv','ihr_costcenter_all.csv'],
    'stgTable':['dailystart','dailyexit','historycostcenterraw','historycostcenterraw'],
    'transformer':['insert_emp_start()','insert_emp_exit()','insert_emp_cc()','insert_emp_cc()'],
    'factTable':['employeestart','employeeexit','employeeccchange','employeeccchange'],
}
mcfg = pd.DataFrame().from_dict(master).set_index('reportKey')

# Loop Through all the configured reports
for index,row in mcfg.iterrows():
    dataSource = row['dataSource']
    reportId = row['reportId']
    reportCol= row['columns']
    reportCsv= row['csvFile']
    stgTable = row['stgTable']
    transformer = row['transformer']
    factTable = row['factTable']

    if dataSource == 'bambooHR':
        print("Downloading from {}, report {}, into csv file......".format(dataSource, reportId))
        extractReport(reportId, reportCsv, reportCol)

    print("Inserting {} into table {}......".format(reportCsv, stgTable))
    insert_to_staging(reportCsv, stgTable)

    print("Executing {} to process data into final table {}......".format(transformer, factTable))
    loadFinalTable(transformer)