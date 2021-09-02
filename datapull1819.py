import pandas as pd
import psycopg2

#Gets the column types of each thing.
def createNewTable(oldTableName, csvLocation, tableName, cur):

    cur.execute(getColumnsQueryString(oldTableName))
    dict = cur.fetchall()
    csv = pd.read_csv(csvLocation, parse_dates=True)
    for i in csv.columns:
        if csv.dtypes[i] == 'object':
            try:
                csv[i] = csv[i].astype('datetime64[ns]')
            except:
                pass

    tuples = getCSVDataTypes(dict, csv.columns, csv)
    createQuery = createTableBasedOnTupleArray(tuples, tableName)
    cur.execute(createQuery)
    return createQuery


def getColumnsQueryString(tableName):
    return "select column_name, data_type from information_schema.columns where table_name = \'" + tableName + "\'"


def getCSVDataTypes(dict, csvCols, csv):
    returnDict = []
    for j in range(0, len(csvCols)):
        found = False
        for i in range(0, len(dict)):
            if csvCols[j] == dict[i][0]:
                returnDict.append((dict[i][0], dict[i][1]))
                found = True
                break
        if not found:
            returnDict.append((csvCols[j], assignDataType(csv.dtypes[csvCols[j]].name)))
    return returnDict


def assignDataType(colType):
    if (colType in ('int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'float64')):
        return 'double precision'
    if (colType in ('object', 'string')):
        return 'character varying'
    return 'date'

def createTableBasedOnTupleArray(tuples, tableName):
    queryString = "DROP TABLE IF EXISTS " + tableName + "; CREATE TABLE " + tableName + "("
    for i in tuples:
        queryString = queryString + i[0] + ' ' + i[1] + ', '
    queryString = queryString[:-2]
    queryString = queryString + ')'
    return queryString

def insertData(csvLocation, tableName, cur):
    with open(csvLocation, 'r') as f:
        f.readline()
        cur.copy_from(f, tableName, sep=',')

def createTableAndInsertData(oldTableName, csvLocation, newTableName, cur):
    createNewTable(oldTableName, csvLocation, newTableName, cur)
    insertData(csvLocation, newTableName, cur)


conn = psycopg2.connect(host="inquire-linux.chpc.utah.edu", dbname="market_scan", user="Enter username here",
                            password="Enter password here")
cur = conn.cursor()
createTableAndInsertData('ccaea083', 'addresses.csv', 'sampleTest', cur)