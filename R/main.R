library(data.table)
library(lubridate)
library(here)
library(stringr)
library(logging)
library(kableExtra)
library(knitr)


logReset()
basicConfig(level='INFO')
lfn = here::here('../data/interim/contractor.log')
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
  fleetDT = data.table::fread(here::here('../data/raw/EMSData_Asset_Data_20190305.csv'), check.names = TRUE)
  stopifnot(!any(fleetDT[,is.na(AHS.Vehicle.ID) | AHS.Vehicle.ID == ""]))
  data.table::setkey(fleetDT, 'AHS.Vehicle.ID')
  
  # Existing IDS Vehicles
  idsDT = data.table::fread(here::here('../data/raw/Vehicle_List_20161020.csv'), check.names = TRUE)
  stopifnot(!any(idsDT[,is.na(EHS.NUMBER) | EHS.NUMBER == ""]))
  data.table::setkey(idsDT, 'EHS.NUMBER')
  
  # Historical vehicles
  histDT = data.table::fread(here::here('../data/raw/qry_UN_HI_CARID_UNID_20190318.csv'), check.names = TRUE)
  data.table::setkey(histDT, 'CARID')
  
  # CSD records of units
  shiftsDT = data.table::fread(here::here('../data/raw/tbl_ShiftInventory_20190320.csv'), check.names = TRUE)
  data.table::setkey(shiftsDT, 'Shift_No')
  
  # Historical record of all vehicle IDs for units logged in in the last 2 years
  unhiDT = data.table::fread(here::here('../data/raw/qry_UN_HI_CARID_full_20190318.csv'), check.names = TRUE)
  data.table::setkey(unhiDT, 'CARID')
  
  return(list(fleetDT=fleetDT, idsDT=idsDT, unhiDT=unhiDT, shiftsDT=shiftsDT, histDT=histDT))
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

filter_nonvehicles <- function(uDT, Vcol=quote(CARID)) {
  uDT = uDT[!grepl('^XV', eval(Vcol))]
  uDT = uDT[!grepl('^C', eval(Vcol))]
  uDT = uDT[!grepl('^FRD', eval(Vcol))]
  uDT = uDT[!grepl('\\d[XYVUROKJIGF]\\d+$', eval(Vcol), perl=TRUE)]
  uDT = uDT[!grepl('^[[:upper:]]{4}\\d$', eval(Vcol), perl=TRUE)]
  
  return(uDT)
}

filter_contr_units <- function(shiftsDT) {
  keep = shiftsDT[,Active.Inactive == 'Active']
  keep = keep & (shiftsDT[,Effective.End == ""] |
                 shiftsDT[,as.Date(Effective.End, format='%Y-%m-%d')] > Sys.Date())
  
  # Contracted ground units
  keep = keep & shiftsDT[,Service_Type] %in% c('Contracted - First Nation', 'Contracted - Municipal/Not-for-Profit', 
                                 'Contracted - First Nation (Private)',
                                 'Contracted - Integrated Fire', 'Contracted - Private', 'Contracted - Air')
  shiftsDT = shiftsDT[keep]
  return(shiftsDT)
}

new_vehicles <- function(fleetDT, idsDT) {
  newDT = fleetDT[!fleetDT[,AHS.Vehicle.ID] %in% idsDT[,Name]]
}


# Load
unpack[fleetDT, idsDT, unhiDT, shiftsDT1, histDT] <- load()

# Filter historical vehicle lists based on vehicle type (no planes, trains or bikes)
unhiDT = filter_nonvehicles(unhiDT)
histDT = filter_nonvehicles(histDT)

# Remove decommissioned vehicles from fleet
fleetDT = filter_decommissioned(fleetDT)

# Identify contractor unit names
shiftsDT = filter_contr_units(shiftsDT1)
shiftsDT = filter_nonvehicles(shiftsDT, quote(Unit_Name))

# Any units not LN in last 2 years
novehDT = shiftsDT[!shiftsDT[,Unit_Name] %in% unhiDT[,UNID]]

# Do they map if you use the alternate?
novehDT[str_detect(Unit_Name, '(-\\d)A(\\d+)$'),alt:=str_replace(Unit_Name, '(-\\d)A(\\d+)$', '\\1B\\2')]
novehDT[str_detect(Unit_Name, '(-\\d)B(\\d+)$'),alt:=str_replace(Unit_Name, '(-\\d)B(\\d+)$', '\\1A\\2')]
novehDT2 = novehDT[!novehDT[,alt] %in% unhiDT[,UNID]]

loginfo(paste0('The following contractor units (and associated A/B alternate) from tbl_ShiftInventory have not logged in\n  (no record in UN_HI in last 2 years):\n', 
        paste(sort(novehDT2[,Unit_Name]), collapse='\n')))

# Move forward with mapped units
setkey(unhiDT, 'UNID')
setkey(shiftsDT, 'Unit_Name')

unhiDT2 = unhiDT[shiftsDT]
unhiDT2 = unhiDT2[!is.na(CARID)]

# Skip units that are already defined in previous Logis Vehicle project iteration
unhiDT2 = unhiDT2[!CARID %in% idsDT[,EHS.NUMBER]]

unhiDT2[,DGroup:=str_extract(UNID, '^[:upper:]{4}')]

# Identify problem units/vehicle combos

# Which vehicles are not frequently used in aggregate
tmpDT1 = unhiDT2[,.(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), 
                 by=CARID][N < 5][order(as.numeric(CARID))]
tmp = kable(tmpDT1, format='markdown')
loginfo(paste0('Infrequently used vehicles:\n', paste0(tmp, collapse="\n")))

# Multiple DGroups/Services
tmpDT2 = unhiDT2[CARID %in% unhiDT2[,uniqueN(DGroup), by=CARID][V1 > 1, CARID], 
        .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';'), 
          Sevices=paste0(unique(Service), collapse=';')), 
        by=.(DGroup, CARID)][order(CARID, -N)]
tmp = kable(tmpDT2, format='markdown')
loginfo(paste0('Vehicles appearing in multiple DGroups:\n', paste0(tmp, collapse="\n")))

tmpDT3 = unhiDT2[CARID %in% unhiDT2[,uniqueN(Service), by=CARID][V1 > 1, CARID], 
                 .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), 
                 by=.(Service, CARID)][order(CARID, -N)]
tmp = kable(tmpDT3, format='markdown')
loginfo(paste0('Vehicles appearing in multiple Services:\n', paste0(tmp, collapse="\n")))

# Vehicles in AHS fleet
tmpDT4 = unhiDT2[CARID %in% fleetDT[,AHS.Vehicle.ID], 
                 .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), 
                 by=CARID][order(CARID, -N)]
tmp = kable(tmpDT4, format='markdown')
loginfo(paste0('Vehicles found in fleet (and not decommissioned):\n', paste0(tmp, collapse="\n")))

contrDT = unhiDT2[, .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), by=.(Service,CARID)]
contrDT2 = unhiDT2[, .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC'))), by=.(UNID,CARID)]

# # Active units in shift inventory, (95% of inactive are virtual BLS/ALS alternate units)
# histDT = filter_nonvehicles(histDT)
# shiftsDT = shiftsDT[shiftsDT[,Unit_Name] %in% histDT[,UNID]]
# shiftsDT[,DGroup := str_match(Unit_Name, '(^[:upper:]{4})-')[,2]]
# shiftsDT[, uniqueN(Service), by=DGroup] # Same service for entire DGroup?
# 
# # Look up vehicles used by DGroup
# setkey(histDT, 'UNID')
# setkey(shiftsDT, 'Unit_Name')
# histDT[,DGroup:=str_extract(UNID, '^[:upper:]{4}')]
# histDT = histDT[shiftsDT]
# contrDT2 = histDT[, .(Service=unique(Service), Division=unique(Division), LastLogon=max(CDTS2), NumLogons=.N, Units=list(unique(UNID))), by=.(DGroup, CARID)]
# 
# # Filter out one offs unless they are they are very recent (i.e. possible transfer of vehicle)
# contrDT3 = contrDT2[, .(NumLogons=sum(NumLogons), LastLogon=max(LastLogon), Units=list(unique(unlist(Units))), DGroups=list(unique(DGroup))), by=.(Service,CARID)]
# 
# # Units only used once in the last 4 months might be a new unit
# keep.date = Sys.Date() - days(120)
# contrDT4 = contrDT3[NumLogons >= 2 | LastLogon >= keep.date]
# 
# # Compare this list of ShiftInventory derived contractor vehicles with the contractor list generated by removing fleet vehicles
# 
# # POssible contractor vehicles in in fleet
# # Are these recently sold vehicles?
# soldDT = contrDT4[!contrDT4[,CARID] %in% contrDT[,CARID]]
# soldDT[soldDT[,CARID] %in% fleetDT[,AHS.Vehicle.ID], CARID]
# 
# # Which vehicles were not used more recently by the contractor
# bad1 = merge(
#   unhiDT[unhiDT[,CARID] %in% soldDT[soldDT[,CARID] %in% fleetDT[,AHS.Vehicle.ID], CARID], 
#          max(LastLogon), by=CARID], 
#   soldDT, by='CARID')[LastLogon < V1, CARID]
# 
# # Remaining unit not found in fleet and not in other contractor list is likely either a mistake/typo, a rarely used vehicle or
# # a vehicle that got passed around in the Service so the single use per unit did not pass threshold
# # Include them
# soldDT[!soldDT[,CARID] %in% fleetDT[,AHS.Vehicle.ID]]
# 
# # Check these contractor vehicles were decommissioned in fleet
# fleetDT[AHS.Vehicle.ID %in% soldDT[,CARID]]
# # 2943 is only contractor vehicle active in fleet, only used once by DD in last couple months, mostly used by contractor
# # So keep
# 
# # Filter bad units
# contrDT4 = contrDT4[CARID != bad1]
# 
# # Check non-fleet vehicles in Unit History that do not align with ShiftInventory
# missDT = contrDT[!contrDT[,CARID] %in% contrDT4[,CARID]]
# 
# # Vehicles that have not logged on in a long time are probably mistake/typos in Unit History or a recently decommissioned vehicle
# keep.date = Sys.Date() - days(200)
# missDT[LastLogon > keep.date]
# 
# # The 400** vehicles are probably missing from Fleet?
# # 975 and 981 might be virtual units that have the wrong UNIT Name format?
# 
# # So don't worry about missDT vehicles, not active or are not contractor vehicles
# 
# # Save contractor vehicles to file
# contrDT4[,id:=1:.N]
# contrDT4[,Units2:=paste0(unlist(Units),collapse=';'), by=id]
# contrDT4[,DGroups2:=paste0(unlist(DGroups),collapse=';'), by=id]
# contrDT4[,Units:=NULL]
# contrDT4[,DGroups:=NULL]
# contrDT4[,id:=NULL]
# setnames(contrDT4, "DGroups2", "DGroups")
# setnames(contrDT4, "Units2", "Units")
# fwrite(contrDT4, here::here('../data/final/contractor_vehicles.csv'), sep=",")
# 
# # Check which vehicles are new since the last Data dump
# contrDT5 = contrDT4[!contrDT4[,CARID] %in% idsDT[,EHS.NUMBER]]
# fwrite(contrDT5, here::here('../data/final/undocumented_contractor_vehicles.csv'), sep=",")
