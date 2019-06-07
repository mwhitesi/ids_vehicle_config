# Direct Delivery vehicles

library(data.table)
library(lubridate)
library(here)
library(stringr)
library(logging)
library(kableExtra)
library(knitr)


logReset()
basicConfig(level='INFO')
lfn = here::here('../data/interim/direct_delivery.log')
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
  oldDT = data.table::fread(here::here('../data/raw/Vehicle_List_20161020_updated_unit_types_20190604.csv'), check.names = TRUE)
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

filter_nonvehicle_carids <- function(vDT, Vcol=quote(CARID)) {
  vDT = vDT[grepl('^\\d+$', CARID)]
  return(vDT)
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

filter_nonvehicles <- function(uDT, Vcol=quote(UNID)) {
  uDT = uDT[!grepl('^XV', eval(Vcol))]
  uDT = uDT[!grepl('^FRD', eval(Vcol))]
  uDT = uDT[!grepl('-\\d[XYVUROKJIGF]\\d+$', eval(Vcol), perl=TRUE)]
  uDT = uDT[!grepl('^[[:upper:]]{4}\\d$', eval(Vcol), perl=TRUE)]
  
  return(uDT)
}

filter_by_service <- function(shiftsDT) {
  keep = shiftsDT[,Active.Inactive == 'Active']
  keep = keep & (shiftsDT[,Effective.End == ""] |
                   shiftsDT[,as.Date(Effective.End, format='%Y-%m-%d')] > Sys.Date())
  
  # Contracted ground units
  keep1 = shiftsDT[,Service_Type] %in% c('Contracted - First Nation', 'Contracted - Municipal/Not-for-Profit', 
                                         'Contracted - First Nation (Private)',
                                         'Contracted - Integrated Fire', 'Contracted - Private', 'Contracted - Air')
  keep2 = shiftsDT[,Service_Type] == 'Direct Delivery'
  
  return(list('contr'=shiftsDT[keep & keep1], 'direct'=shiftsDT[keep & keep2]))
}

filter_shifts <- function(shiftsDT) {
  keep = shiftsDT[,Active.Inactive == 'Active']
  keep = keep & (shiftsDT[,Effective.End == ""] |
                   shiftsDT[,as.Date(Effective.End, format='%Y-%m-%d')] > Sys.Date())
  
  return(shiftsDT[keep])
}

new_vehicles <- function(fleetDT, idsDT) {
  newDT = fleetDT[!fleetDT[,AHS.Vehicle.ID] %in% idsDT[,Name]]
}

prep_lookup_table <- function(file) {
  mapDT = data.table::fread(here::here(file), check.names = TRUE)
  data.table::setkey(mapDT, 'AHS.Vehicle.ID')
  
  mapDT = filter_decommissioned(mapDT)
  
  # Ambulances are only fully populated dataset
  stopifnot(all(mapDT$Classification != ''))
  mapDT = mapDT[grepl('^AMB', Classification)]
  
  # Relevent columns needed to assign a unit type
  cols = c("VehicleType", 
           "StockLevel", "StretcherConfig", "StretcherType", 
           "PtCompSize", "SeatingConfig", "SeatingConfig2")
  
  map2 = mapDT[, cols, with=F]
  map2 = map2[,cols:=lapply(.SD, tolower), with=F, .SDcols=cols]
  map2 = map2[!duplicated(mapDT[,cols, with=F])]
  
  data.table::fwrite(map2, here::here('../data/interim/EMSData_unit_type_mapping.csv'))
  
}

output_staging_table <- function(dt) {
  mydt = dt[,.(Vehicle,VehicleType)]
  mydt[,`:=`(Active="true", EffectiveStart=Sys.Date())]
  data.table::fwrite(mydt, here::here('../data/interim/initial_ambulance_staging_table.csv'))
}


# Load
unpack[fleetDT, oldDT, unhiDT, shiftsDT, vehicDT] <- load()

# Keep road vehicles
vehicDT = filter_nonvehicle_carids(vehicDT)

# Identify active vehicles
unusedDT = vehicDT[!vehicDT$CARID %in% unhiDT$CARID] # Some of these unused maybe brand new, so keep for later

# BRANCH POINT, DEF_VEHIC is missing several vehicles. Use UN_HI only
vehicDT = data.table(CARID=unhiDT[,unique(CARID)])
vehicDT = filter_nonvehicle_carids(vehicDT)
unhi.vehicDT = unhiDT[vehicDT] # Join to history
unhi.vehicDT = unhi.vehicDT[grepl('-\\d[AB]\\d+$', UNID)] # Only ambulances
unhi.vehicDT = unhi.vehicDT[nchar(CARID) <= 4]
data.table::setkey(unhi.vehicDT, 'UNID')

# Only active units based on Shift Inventory
unpack[contrDT, directDT] = filter_by_service(shiftsDT)
#shiftsDT = filter_shifts(shiftsDT)
directDT = directDT[grepl('-\\d[AB]\\d+$', Unit_Name)]

unhi.vehicDT = unhi.vehicDT[UNID %in% directDT$Unit_Name]

# Remove vehicles that have not been consistently used more than once per dgroup
unhi.vehicDT[,dgroup:=str_extract(UNID, "^(.{4})")]

un.vehic.countsDT = unhi.vehicDT[,.(.N, LastLogon=max(CDTS2)),by=.(dgroup, CARID)]
data.table::setorder(un.vehic.countsDT, -'N', -'LastLogon', 'dgroup')

# Look for an elbow in the plot (difference between routine use and abnormal use?)
plot(ecdf(un.vehic.countsDT[,N]), xlim=c(0,50))

routineDT = un.vehic.countsDT[N > 1]
routineDT = routineDT[,.(Dgroups=paste0(unique(dgroup), collapse=';'), N=sum(N)), by=CARID]

# Rarely used vehicles
rareDT = un.vehic.countsDT[N == 1]
rareDT = rareDT[,.(Dgroups=paste0(unique(dgroup), collapse=';'), N=sum(N)), by=CARID]
rareDT = rareDT[!rareDT$CARID %in% routineDT$CARID]


# Join to fleet record after removing inactive and decommissioned records
# Note: some marked inactive in fleet maybe active in CAD (e.g. if they moved to contractor service), 
# however, I have decided not to include theses cases as a precaution
active.fleetDT = filter_decommissioned(fleetDT)

any(duplicated(active.fleetDT[,AHS.Vehicle.ID]))

fleet.vehicDT = merge(routineDT, active.fleetDT, by.x='CARID', by.y='AHS.Vehicle.ID', all.x=TRUE)
data.table::setkey(fleet.vehicDT, 'CARID')

# I prebuilt a data.table that assigns row value combinations in select columns from fleetDT to Unit_Types
# NOTE: DO NOT USE THIS AS A GUIDE. THe data in EMS fleet is extremetly inconsistent. Most ambulances have an identical setup
# so I assigned most row value combinations that are very likely to contain mistakes to what I think is the proper type.
# A SC114 should be an ambulance III, have 3 compartment fixed seats, a compartment size of 164 and a power load and stryker power-pro XT stretcher setup.
# Most variations have been confirmed to be data entry errors
# This table will need to be updated as needed
# You can generate a starting table using the function prep_lookup_table() to get row value combinations.
mapDT = data.table::fread(here::here('../data/interim/EMSData_unit_type_mapping_20190604.csv'), check.names = TRUE, colClasses = 'character')

# Convert all inputs to lowercase
cols = head(colnames(mapDT), -1)
all(cols %in% colnames(fleet.vehicDT))

fleet.vehicDT[,(cols):=lapply(.SD, tolower), .SDcols=cols]

fleet.vehicDT = merge(fleet.vehicDT, mapDT, all.x=TRUE, by=cols)

fleet.vehicDT = merge(fleet.vehicDT, oldDT, all.x=TRUE, by.x="CARID", by.y="EHS.NUMBER")

valid.vehicDT = fleet.vehicDT[!is.na(Updated) | UnitType != 'UNKNOWN']
valid.vehicDT[,`:=`(VehicleType = ifelse(is.na(Updated), UnitType, Updated), Vehicle=CARID)]
gaps.vehicDT = fleet.vehicDT[is.na(Updated) & UnitType == 'UNKNOWN']

# Checked and obtained information individually for a couple of the gap vehicles 2019-05-15
gaps.vehicDT[CARID == '3089', UnitType:='SC113']
gaps.vehicDT[CARID == '3122', UnitType:='SC113']
gaps.vehicDT[CARID == '3148', UnitType:='SC113']
gaps.vehicDT[CARID == '3151', UnitType:='SC113']
gaps.vehicDT[CARID == '3158', UnitType:='SC113']
gaps.vehicDT[CARID == '3165', UnitType:='SC113']
gaps.vehicDT[CARID == '3182', UnitType:='SC113']
gaps.vehicDT[CARID == '3112', UnitType:='DS013']

notgaps.vehicDT = gaps.vehicDT[UnitType != 'UNKNOWN']
notgaps.vehicDT[,`:=`(VehicleType = ifelse(is.na(Updated), UnitType, Updated), Vehicle=CARID)]
valid.vehicDT = rbindlist(list(valid.vehicDT, notgaps.vehicDT))


output_staging_table(valid.vehicDT)
