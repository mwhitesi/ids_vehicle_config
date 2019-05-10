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
  oldDT = data.table::fread(here::here('../data/raw/Vehicle_List_20161020.csv'), check.names = TRUE)
  stopifnot(!any(oldDT[,is.na(EHS.NUMBER) | EHS.NUMBER == ""]))
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

filter_nonvehicles <- function(uDT, Vcol=quote(CARID)) {
  uDT = uDT[!grepl('^XV', eval(Vcol))]
  uDT = uDT[!grepl('^FRD', eval(Vcol))]
  uDT = uDT[!grepl('\\d[XYVUROKJIGF]\\d+$', eval(Vcol), perl=TRUE)]
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


# Load
unpack[fleetDT, oldDT, unhiDT, shiftsDT, vehicDT] <- load()

# Keep road vehicles
vehicDT = filter_nonvehicle_carids(vehicDT)

# Identify active vehicles
unusedDT = vehicDT[!vehicDT$CARID %in% unhiDT$CARID] # Some of these unused maybe brand new, so keep for later
unhi.vehicDT = unhiDT[vehicDT] # Join to history
unhi.vehicDT = unhi.vehicDT[grepl('-\\d[AB]\\d+$', UNID)] # Only ambulances
data.table::setkey(unhi.vehicDT, 'UNID')

# Only active units based on Shift Inventory
unpack[contrDT, directDT] = filter_by_service(shiftsDT)
#shiftsDT = filter_shifts(shiftsDT)
directDT = directDT[grepl('-\\d[AB]\\d+$', Unit_Name)]

unhi.vehicDT = unhi.vehicDT[UNID %in% directDT$Unit_Name]

# Remove vehicles that have not been consistently used more than once per unit
unhi.vehicDT[,generic:=str_replace(UNID, "-(\\d)[AB](\\d+)$", "-\\1_\\2")]
unhi.vehicDT[,generic2:=str_replace(generic, "-[12]_(\\d+)$", "-DN_\\1")]

un.vehic.countsDT = unhi.vehicDT[,.(.N, LastLogon=max(CDTS2)),by=.(generic, CARID)]
data.table::setorder(un.vehic.countsDT, -'N', -'LastLogon', 'generic')

# Look for an elbow in the plot (difference between routine use and abnormal use?)
plot(ecdf(un.vehic.countsDT[,N]), xlim=c(0,50))
abline(v=7)

routineDT = un.vehic.countsDT[N > 1]
routineDT = routineDT[,.(Units=paste0(unique(generic), collapse=';')), by=CARID]

# Rarely used vehicles
rareDT = un.vehic.countsDT[N = 1]
rareDT = rareDT[,.(Units=paste0(unique(generic), collapse=';')), by=CARID]
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
mapDT = data.table::fread(here::here('../data/interim/EMSData_unit_type_mapping_20190507.csv'), check.names = TRUE, colClasses = 'character')

# Convert all inputs to lowercase
cols = head(colnames(mapDT), -1)
all(cols %in% colnames(fleet.vehicDT))

fleet.vehicDT[,(cols):=lapply(.SD, tolower), .SDcols=cols]

fleet.vehicDT = merge(fleet.vehicDT, mapDT, all.x=TRUE, by=cols)

fleet.vehicDT = merge(fleet.vehicDT, oldDT, all.x=TRUE, by.x="CARID", by.y="EHS.NUMBER")


# # Filter historical vehicle lists based on vehicle type (no planes, trains or bikes)
# unhiDT = filter_nonvehicles(unhiDT)
# 
# # Remove decommissioned vehicles from fleet
# fleetDT = filter_decommissioned(fleetDT)
# 
# # Identify contractor/dd unit names
# unpack[contrDT, directDT] = filter_by_service(shiftsDT)
# directDT = filter_nonvehicles(directDT, quote(Unit_Name))
# 
# # Any units not LN in last 2 years
# nolnDT = directDT[!directDT[,Unit_Name] %in% unhiDT[,UNID]]
# 
# # Do they map if you use the alternate?
# nolnDT[str_detect(Unit_Name, '(-\\d)A(\\d+)$'),alt:=str_replace(Unit_Name, '(-\\d)A(\\d+)$', '\\1B\\2')]
# nolnDT[str_detect(Unit_Name, '(-\\d)B(\\d+)$'),alt:=str_replace(Unit_Name, '(-\\d)B(\\d+)$', '\\1A\\2')]
# nolnDT2 = nolnDT[!nolnDT[,alt] %in% unhiDT[,UNID]]
# 
# loginfo(paste0('The following direct delivery units (and associated A/B alternate) from tbl_ShiftInventory have not logged in\n  (no record in UN_HI in last 2 years):\n', 
#                paste(sort(nolnDT2[,Unit_Name]), collapse='\n')))
# 
# # Map vehicles to units for direct and contractor vehicles
# setkey(unhiDT, 'UNID')
# setkey(directDT, 'Unit_Name')
# 
# unhiDT2 = unhiDT[directDT]
# unhiDT2 = unhiDT2[!is.na(CARID)]
# unhiDT2[,DGroup:=str_extract(UNID, '^[:upper:]{4}')]
# 
# setkey(contrDT, 'Unit_Name')
# unhiDT3 = unhiDT[contrDT]
# unhiDT3 = unhiDT3[!is.na(CARID)]
# contrDT = unhiDT3[,.(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), by=CARID]
# 
# # Identify problem units/vehicle combos
# 
# # Which vehicles are not frequently used in aggregate
# tmpDT1 = unhiDT2[,.(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), 
#                  by=CARID][N < 5][order(as.numeric(CARID))]
# tmp = kable(tmpDT1, format='markdown')
# loginfo(paste0('Infrequently used vehicles:\n', paste0(tmp, collapse="\n")))
# 
# # Which vehicles are also being reported as being used by contractor unit
# tmpDT2 = unhiDT2[CARID %in% contrDT[,CARID],
#                  .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), 
#                  by=CARID][order(CARID, -N)]
# tmpDT2 = merge(tmpDT2, contrDT, by='CARID', all.x=TRUE, suffixes = c('.d','.c'))
# tmp = kable(tmpDT2, format='markdown')
# loginfo(paste0('Vehicles appearing in Contractor Services:\n', paste0(tmp, collapse="\n")))
# 
# unhiDT4 = unhiDT2[!CARID %in% tmpDT2[N.c>N.d,CARID]]
# dd.vehicles = sort(unique(unhiDT4[,CARID]))
# loginfo(paste0('Direct Delivery vehicles:\n', paste0(dd.vehicles, collapse="\n")))
# 
# # Which vehicles do we not have config information for (documented)
# undoc.vehicles = dd.vehicles[!dd.vehicles %in% idsDT[,EHS.NUMBER]]
# 
# loginfo(paste0('Vehicles with no config information:\n', paste0(undoc.vehicles, collapse="\n")))
# 
# # Which vehicles are in fleet database
# unkn.vehicles = undoc.vehicles[!undoc.vehicles %in% fleetDT[,AHS.Vehicle.ID]]
# fleet.vehicles = undoc.vehicles[undoc.vehicles %in% fleetDT[,AHS.Vehicle.ID]]
# 
# 
# tmpDT3 = unhiDT4[CARID %in% unkn.vehicles, 
#                  .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')),
#                  by=CARID]
# tmp = kable(tmpDT3, format='markdown')
# loginfo(paste0('Vehicles not found in fleet database:\n', paste0(tmp, collapse="\n")))
# 
# # Check information on undocumented vehicles found in fleet (vehicles with no prior config information)
# fleetDT = fleetDT[AHS.Vehicle.ID %in% fleet.vehicles]
# fleetDT[,.N,by=Classification]
# 
# # Tackle by Vehicle Classifcation to determine gaps in information (needed information changes based on vehicle type)
# 
# # Most ambulances (with exception of rare specialty units) should have the same configuration
# data.cols = c('IsActive', 'VehicleType', 'InServicedate', 'StockLevel', 'Division', 'StretcherConfig', 'StretcherType',
#               'PtCompSize','SeatingConfig', 'SeatingConfig2')
# ambDT = fleetDT[Classification == "AMB-Primary"]
# 
# missing.data = apply(ambDT[,data.cols, with=FALSE], 1, function(r) any(is.na(r)) | any(r == ""))
# 
# 
# is_stardard_amb_config <- function(arow) {
#   
#   r = c(arow["IsActive"] == TRUE,
#         arow["VehicleType"] == "Ambulance Type III",
#         arow["StockLevel"] == "ALS",
#         arow["StretcherConfig"] == "Power Load",
#         grepl('stryker.+power.+pro.+xt', arow["StretcherType"], ignore.case = TRUE),
#         arow["PtCompSize"] == "164",
#         arow["SeatingConfig"] == "Regular - 3",
#         arow["SeatingConfig2"] == "Folding Seats - 2"
#         )
#   
#   return(all(r))
# }
# sum(apply(ambDT[!missing.data, data.cols, with=FALSE], 1, is_stardard_amb_config))
# 
# # NATs
# natDT = unhiDT2[grepl('-\\dT\\d+$', UNID), .(.N, Units=list(unique(UNID)), Divisions=list(unique(Division)), LastLogon=max(CDTS2)), by=.(CARID)]
# setkey(natDT, 'CARID')
# 
# # all
# natDT[!natDT[,CARID] %in% idsDT[,EHS.NUMBER]]
# 
# # not ambulances
# natDT[!natDT[,CARID] %in% idsDT[,EHS.NUMBER]][nchar(CARID)>4]
# 
# natDT[nchar(CARID)>4 & substr(Divisions, 1, 12) == 'AHS - North ', .(CARID, N, Units)]
