# install.packages("RCurl")
# install.packages("hash")
# install.packages("urltools")
require(hash)
require(urltools)
require(RCurl)
require(lubridate) # date utils
require(tidyverse)
options(show.error.locations = TRUE)

ROOT_DIR <- file.path(getwd(), "cdec-data")
BASE_URL <- "http://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet"

if( !dir.exists(ROOT_DIR) ) {
  dir.create(ROOT_DIR)
}

stations <- c(
  hash(
    Stations="SRM",
    SensorNums="20",
    dur_code="E",
    Start="2006-06-27",
    End="2019-03-12"
  ),
  hash(
    Stations="PCN",
    SensorNums="20",
    dur_code="E",
    Start="2000-07-14",
    End="2019-03-12"
  ),
  hash(
    Stations="KIG",
    SensorNums="20",
    dur_code="E",
    Start="2000-02-14",
    End="2019-03-12"
  )
)

dailyCsvData <- function(file, stationId) {
  agFile <- str_replace(file, ".csv", "-daily.csv")
  if( file.exists(agFile) ) {
    print(paste("already exists:", agFile))
    return()
  }
  
  orgData <- read_csv(file)
  agData <- orgData %>%
    rename(date = "DATE TIME", value = "VALUE", measurement_unit = "UNITS") %>%
    filter( !is.na(as.numeric(value)) ) %>%
    mutate(date = as.Date(date, "%Y%m%d")) %>%
    select(date, value, measurement_unit)  %>%
    group_by(date, measurement_unit) %>%
    summarize(value = mean(as.numeric(value))) %>%
    add_column(data_source = "cdec") %>%
    add_column(station_name = stationId) %>%
    add_column(measurement_name = "flow") %>%
    add_column(measurement_precision = "") %>%
    add_column(quality_flag = "") %>%
    select(
      station_name, 
      date,
      value,
      measurement_name,
      measurement_unit,
      measurement_precision,
      data_source,
      quality_flag
    ) 
    
  write_csv(agData, agFile)
}

downloadYear <- function(info) {
  url <- BASE_URL
  year <- format(as.Date(info$Start), "%Y")
  
  stationDir <- file.path(ROOT_DIR, info$Stations)
  if( !dir.exists(stationDir) ) {
    dir.create(stationDir)
  }
  
  filepath <- file.path(stationDir,  paste(info$Stations, "flow", paste(year, ".csv", sep=""), sep="-"))
  if( file.exists(filepath) ) {
    print(paste("already exists:", filepath))
    dailyCsvData(filepath, info$Stations)
    return()
  }
    
  for( key in keys(info)) {
    url <- param_set(url, key = key, value = info[[key]])
  }
  
  download.file(url, destfile=filepath,method="libcurl")
  dailyCsvData(filepath, info$Stations)
}


for( station in stations ) {
  end <- as.Date(station$End)
  startYear <- format(as.Date(station$Start), "%Y")

  currentDate <- as.Date(paste(startYear,"01","01",sep="-"))
  currentEndDate <- currentDate %m+% years(1)

  while( currentDate <= end ) {
    downloadYear(hash(
      Stations=station$Stations,
      SensorNums=station$SensorNums,
      dur_code=station$dur_code,
      Start=as.character(currentDate),
      End=as.character(currentEndDate)
    ))
    currentDate <- currentEndDate
    currentEndDate <- currentDate %m+% years(1)
  }
}



