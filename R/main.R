library(data.table)
library(lubridate)
library(here)
library(stringr)


unpack <- structure(NA,class="result")
"[<-.result" <- function(x,...,value) {
  args <- as.list(match.call())
  args <- args[-c(1:2,length(args))]
  length(value) <- length(args)
  for(i in seq(along=args)) {
    a <- args[[i]]
    if(!missing(a)) eval.parent(substitute(a <- v,list(a=a,v=value[[i]])))
  }
  x
}

load <- function() {
  
  # New fleet data
  fleetDT = data.table::fread(here::here('../data/raw/EMSData_Asset_Data_20190305.csv'), check.names = TRUE)
  stopifnot(!any(fleetDT[,is.na(AHS.Vehicle.ID) | AHS.Vehicle.ID == ""]))
  data.table::setkey(fleetDT, 'AHS.Vehicle.ID')
  
  # Existing IDS Vehicles
  idsDT = data.table::fread(here::here('../data/raw/Unit_2019-03-08_15-19-07.csv'), check.names = TRUE)
  stopifnot(!any(idsDT[,is.na(Name) | Name == ""]))
  data.table::setkey(idsDT, 'Name')
  
  # Historical vehicles
  unhiDT = data.table::fread(here::here('../data/raw/qry_UN_HI_CARID_20190315.csv'), check.names = TRUE)
  data.table::setkey(unhiDT, 'carid')
  
  return(list(fleetDT=fleetDT, idsDT=idsDT, unhiDT=unhiDT))
}

checks <- function(fleetDT, idsDT) {
  missing = idsDT[!idsDT[,Name] %in% fleetDT[,AHS.Vehicle.ID]]
  
  return(missing) 
}

filter_decommissioned <- function(fleetDT) {
  
  fleetDT = fleetDT[!(grepl("^Decommis", Classification) | DecommisionDate != "" | grepl("^Decommis", StockLevel))] # Decommissioned
  fleetDT = fleetDT[!grepl("^Trailer", Classification)] # Trailors
  fleetDT = fleetDT[!grepl("^Golf", VehicleType, ignore.case = T)] # Golf carts
  
  # Remove some random test db entries
  fleetDT = fleetDT[!(AHS.Vehicle.ID %in% c('Spare','test123','test(ALS)'))]
  
  return(fleetDT)
}

filter_active <- function(fleetDT) {
  # Remove units that have not been logged on in the last 2 years and are not new
  
  activeDT = data.table::fread(here::here('../data/raw/Logis_CARID_export_from_CAD_ground_20190311.csv'), check.names = TRUE)
  data.table::setkey(activeDT, 'Name')
  
  keep = fleetDT[,AHS.Vehicle.ID] %in% activeDT[,Name] # Logged into CAD in last 2 years
  new.date = Sys.Date() - days(365)
  new.year = as.character(year(new.date))
  keep = keep | fleetDT[,as.Date(InServicedate, format='%Y-%m-%d')] > new.date # Put into service in last year
  keep = keep | fleetDT[,Year] >= new.year # Model number is 1 year old
  
  return(fleetDT[keep])
}

filter_nonvehicles <- function(unhiDT) {
  unhiDT = unhiDT[!grepl('^XV', carid)]
  unhiDT = unhiDT[!grepl('^FRD', UNID)]
  unhiDT = unhiDT[!grepl('\\-\\d[XYVUROKJIGF]', UNID, perl=TRUE)]
  
  return(unhiDT)
}

new_vehicles <- function(fleetDT, idsDT) {
  newDT = fleetDT[!fleetDT[,AHS.Vehicle.ID] %in% idsDT[,Name]]
}



unpack[fleetDT, idsDT, unhiDT] <- load()
# activeDT <- filter_decommissioned(fleetDT)
# activeDT <- filter_active(activeDT)
# 
# newDT = new_vehicles(activeDT, idsDT)

# Extract the set of contractor (or highly likely to be contractor) vehicles
# This set will also include typos during MPS login or AHS units missing from fleet database
unhiDT = filter_nonvehicles(unhiDT)
contrDT = unhiDT[!unhiDT[,carid] %in% fleetDT[,AHS.Vehicle.ID] & NumLogon >= 5]


