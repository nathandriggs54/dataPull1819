import pandas as pd
from sqlalchemy import create_engine
from pandas.io import sql

def generateOneTable(tableName, engine):
    sql.execute('DROP TABLE IF EXISTS ' + tableName, engine)
    df = pd.read_csv(tableName + ".csv")
    df.to_sql(tableName, con=engine)

def generateCCAEandMDCR(engine, extension, suffix18, suffix19):
    generateOneTable("ccae" + extension + suffix18, engine)
    generateOneTable("ccae" + extension + suffix19, engine)
    generateOneTable("mdcr" + extension + suffix18, engine)
    generateOneTable("mdcr" + extension + suffix19, engine)


dbname = "market_scan"
host = "inquire-linux.chpc.utah.edu"
port = "5432"
user = "(Enter Username Here)"
password = "(Enter Password Here)"
connectionString = "postgresql://" + user + ":" + password + "@" + host + ":" + port + "/" + dbname
engine = create_engine(connectionString, echo=False)

suffix18 = "182"
suffix19 = "192"

generateCCAEandMDCR(engine, "a", suffix18, suffix19)
generateCCAEandMDCR(engine, "d", suffix18, suffix19)
generateCCAEandMDCR(engine, "f", suffix18, suffix19)
generateCCAEandMDCR(engine, "i", suffix18, suffix19)
generateCCAEandMDCR(engine, "o", suffix18, suffix19)
#generateCCAEandMDCR(engine, "p", suffix18, suffix19)
generateCCAEandMDCR(engine, "s", suffix18, suffix19)
generateCCAEandMDCR(engine, "t", suffix18, suffix19)



