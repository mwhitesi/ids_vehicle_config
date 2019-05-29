# SUV vehicles

library(data.table)
library(lubridate)
library(here)
library(stringr)
library(logging)
library(kableExtra)
library(knitr)


logReset()
basicConfig(level='INFO')
lfn = here::here('../data/interim/suv.log')
if (file.exists(lfn)) 
  file.remove(lfn)
addHandler(writeToFile, file=lfn, level='DEBUG')


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
  fleetDT = data.table::fread(here::here('../data/raw/EMSData_Asset_Data_20190506.csv'), check.names = TRUE)
  stopifnot(!any(fleetDT[,is.na(AHS.Vehicle.ID) | AHS.Vehicle.ID == ""]))
  data.table::setkey(fleetDT, 'AHS.Vehicle.ID')
  
  # Existing IDS Vehicles
  oldDT = data.table::fread(here::here('../data/raw/Vehicle_List_20161020_updated_unit_types_20190513.csv'), check.names = TRUE)
  stopifnot(!any(oldDT[,is.na(EHS.NUMBER) | EHS.NUMBER == ""]))
  oldDT = unique(oldDT, by=c('EHS.NUMBER', 'NAME'))
  data.table::setkey(oldDT, 'EHS.NUMBER')
  
  # Historical vehicles
  vehicDT = data.table::fread(here::here('../data/raw/qry_DEF_VEHIC_20190507.csv'), check.names = TRUE)
  data.table::setkey(vehicDT, 'CARID')
  
  # CSD records of units
  shiftsDT = data.table::fread(here::here('../data/raw/tbl_ShiftInventory_20190508.csv'), check.names = TRUE)
  data.table::setkey(shiftsDT, 'Shift_No')
  
  # Historical record of all vehicle IDs for units logged in in the last 2 years
  unhiDT = data.table::fread(here::here('../data/raw/qry_UN_HI_UNIT_WKLOAD_CARID_20190510.csv'), check.names = TRUE)
  data.table::setkey(unhiDT, 'CARID')
  
  return(list(fleetDT=fleetDT, oldDT=oldDT, unhiDT=unhiDT, shiftsDT=shiftsDT, vehicDT=vehicDT))
}

filter_decommissioned <- function(fleetDT) {
  
  fleetDT = fleetDT[IsActive == TRUE]
  fleetDT = fleetDT[!(grepl("^Decom", Classification) | DecommisionDate != "" | grepl("^Decom", StockLevel))] # Decommissioned
  fleetDT = fleetDT[!grepl("^Trailer", Classification)] # Trailors
  fleetDT = fleetDT[!grepl("^Golf", VehicleType, ignore.case = T)] # Golf carts
  
  # Remove some random test db entries
  fleetDT = fleetDT[!(AHS.Vehicle.ID %in% c('Spare','test123','test(ALS)'))]
  
  return(fleetDT)
}

filter_shifts <- function(shiftsDT) {
  keep = shiftsDT[,Active.Inactive == 'Active']
  keep = keep & (shiftsDT[,Effective.End == ""] |
                   shiftsDT[,as.Date(Effective.End, format='%Y-%m-%d')] > Sys.Date())
  
  return(shiftsDT[keep])
}

filter_fleet_suvs <- function(fleetDT) {
  suv_patterns = paste(c('tahoe', 'explorer', 'yukon'), collapse = '|')
  class_patterns = paste(c('suv'), collapse = '|')
  type_patterns = paste(c('pru','supervisor','admin','community'), collapse = '|')
  stock_patterns = paste(c('pru', 'admin', 'community'), collapse = '|')
  
  
  fleetDT[str_detect(Model, regex(suv_patterns, ignore_case = T)) |
          str_detect(Classification,  regex(class_patterns, ignore_case = T)) |
          str_detect(VehicleType,  regex(class_patterns, ignore_case = T)) |
          str_detect(Division, regex("Community Care", ignore_case = T)) | 
          str_detect(StockLevel, regex(stock_patterns, ignore_case = T))]
}

output_staging_table <- function(dt) {
  mydt = dt[,.(Vehicle,VehicleType)]
  mydt[,`:=`(Active="true", EffectiveStart=Sys.Date())]
  data.table::fwrite(mydt, here::here('../data/interim/initial_suv_staging_table.csv'))
}


# Load
unpack[fleetDT, oldDT, unhiDT, shiftsDT, vehicDT] <- load()

# Identify potential SUVs from UN_HI
suvDT = unhiDT[grepl('-\\d[PSDMLW]\\d+$', UNID) & nchar(CARID) != 4 & !grepl('^9\\d{2}$', CARID)]
data.table::setkey(suvDT, 'UNID')

# Only active units based on Shift Inventory
active.shiftsDT = filter_shifts(shiftsDT)
suvDT = suvDT[UNID %in% active.shiftsDT[,Unit_Name]]

# Compare against fleet SUVs
active.fleetDT = filter_decommissioned(fleetDT)
suv.fleetDT = filter_fleet_suvs(active.fleetDT)
suv.fleetDT = suv.fleetDT[nchar(AHS.Vehicle.ID) != 4]


# Include vehicles that have been used recently 
countDT = suvDT[as.Date(CDTS2) > as.Date('2019-01-01'), .N, by=CARID]
routineDT = countDT[N > 1]
missingDT = routineDT[!CARID %in% suv.fleetDT[,AHS.Vehicle.ID]]

final2DT = missingDT[,.(VehicleType='AA002S', Vehicle=CARID)]
finalDT = suv.fleetDT[,.(VehicleType='AA002S', Vehicle=AHS.Vehicle.ID)]
finalDT = rbindlist(list(finalDT, final2DT))


output_staging_table(finalDT)
