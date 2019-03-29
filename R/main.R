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
  keep1 = shiftsDT[,Service_Type] %in% c('Contracted - First Nation', 'Contracted - Municipal/Not-for-Profit', 
                                         'Contracted - First Nation (Private)',
                                         'Contracted - Integrated Fire', 'Contracted - Private', 'Contracted - Air')
  return(list('contr'=shiftsDT[keep & keep1], 'direct'=shiftsDT[keep & !keep1]))
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
unpack[shiftsDT, directDT] = filter_contr_units(shiftsDT1)
shiftsDT = filter_nonvehicles(shiftsDT, quote(Unit_Name))
directDT = filter_nonvehicles(directDT, quote(Unit_Name))

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

setkey(directDT, 'Unit_Name')
unhiDT3 = unhiDT[directDT]
unhiDT3 = unhiDT3[!is.na(CARID)]
directDT = unhiDT3[,.(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), by=CARID]

# Skip units that are already defined in previous Logis Vehicle project iteration
#unhiDT2 = unhiDT2[!CARID %in% idsDT[,EHS.NUMBER]]

unhiDT2[,DGroup:=str_extract(UNID, '^[:upper:]{4}')]

# Identify problem units/vehicle combos

# Which vehicles are not frequently used in aggregate
tmpDT1 = unhiDT2[,.(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), 
                 by=CARID][N < 5][order(as.numeric(CARID))]
tmp = kable(tmpDT1, format='markdown')
loginfo(paste0('Infrequently used vehicles:\n', paste0(tmp, collapse="\n")))

# KEEP all

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

tmpDT4 = unhiDT2[CARID %in% directDT[,CARID],
                 .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), 
                 by=CARID][order(CARID, -N)]
tmpDT4 = merge(tmpDT4, directDT, by='CARID', all.x=TRUE, suffixes = c('.c','.d'))
tmp = kable(tmpDT4, format='markdown')
loginfo(paste0('Vehicles appearing in Direct Delivery Services:\n', paste0(tmp, collapse="\n")))

# Remove all vehicles where the count is higher for direct delivery
omit_vehicles = c(tmpDT4[N.d > N.c, CARID], 10095)

# Assign a unique Service
tmpDT31 = tmpDT3[,.(mxN=max(N),mxL=max(LastLogon)),by=CARID]
tmpDT31 = tmpDT31[tmpDT3, on='CARID']

# Remove all duplicate service/vehicle assignments that have fewer records
omit_pairs = tmpDT31[N != mxN, .(CARID, Service)]

# Vehicles in AHS fleet
tmpDT5 = unhiDT2[CARID %in% fleetDT[,AHS.Vehicle.ID], 
                 .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), 
                 by=CARID][order(CARID, -N)]
tmp = kable(tmpDT5, format='markdown')
loginfo(paste0('Vehicles found in fleet (and not decommissioned):\n', paste0(tmp, collapse="\n")))

# Clean up and output
contrDT = unhiDT2[, .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), by=.(Service,CARID)]
setkey(contrDT, CARID, Service)
contrDT = contrDT[!CARID %in% omit_vehicles]
contrDT = contrDT[!omit_pairs, on=.(CARID, Service)]
contrDT[,New:=ifelse(CARID %in% idsDT[,EHS.NUMBER], 0, 1)]

setorder(contrDT, 'Service')

fwrite(contrDT, here::here(paste0('../data/final/all_contractor_vehicles_',Sys.Date(),'.csv')), sep=",")
