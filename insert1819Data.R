createTable <- function(db, tableName, dataframe)
{
  if (DBI::dbExistsTable(db, SQL(tableName)))
  {
    DBI::dbRemoveTable(db, SQL(tableName))
  }
  DBI::dbWriteTable(db, SQL(tableName), dataframe)
}

generateOneTable <- function(db, extension, suffix, path_to_database, insurance_type)
{
  name <- paste(insurance_type, extension, suffix, sep = '')
  dataframe <- read.csv(paste(path_to_database, name, '.csv', sep = ''))
  createTable(db, name, dataframe)
}

generateCCAEandMDCR <- function(db, extension, suffix18, suffix19, path_to_database)
{
  generateOneTable(db, extension, suffix18, path_to_database, 'ccae')
  generateOneTable(db, extension, suffix19, path_to_database, 'ccae')
  generateOneTable(db, extension, suffix18, path_to_database, 'mdcr')
  generateOneTable(db, extension, suffix19, path_to_database, 'mdcr')
}

library(DBI)

library(getPass)

library(RPostgreSQL)

library(sqldf)

#Database information and credentials.
db <-DBI::dbConnect(
  
  dbDriver(drvName = "PostgreSQL"),
  
  dbname="market_scan",
  
  host="inquire-linux.chpc.utah.edu",
  
  port=5432,
  
  user = 'u0632236',
  
  password = getPass("yu0rkumalfcvmivylzbjSkb1nluHlpfm"))

#Setting variables as shortcuts. All of these may be changed when we get the file names.

#Might want to setwd() before you do this.

suffix18 <- '181'

suffix19 <- '191'

path_to_database <- "./"

##########################################################################

#This script reads the ccae csvs into r.

generateCCAEandMDCR(db, 'a', suffix18, suffix19, path_to_database)
generateCCAEandMDCR(db, 'd', suffix18, suffix19, path_to_database)
generateCCAEandMDCR(db, 'f', suffix18, suffix19, path_to_database)
generateCCAEandMDCR(db, 'i', suffix18, suffix19, path_to_database)
generateCCAEandMDCR(db, 'o', suffix18, suffix19, path_to_database)
#generateCCAEandMDCR(db, 'p', suffix18, suffix19, path_to_database) We probably won't have a 'p' table, but include just in case.
generateCCAEandMDCR(db, 's', suffix18, suffix19, path_to_database)
generateCCAEandMDCR(db, 't', suffix18, suffix19, path_to_database)






