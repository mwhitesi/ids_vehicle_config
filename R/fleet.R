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
  fleetDT = data.table::fread(here::here('../data/raw/EMSData_Asset_Data_20190305.csv'), check.names = TRUE)
  stopifnot(!any(fleetDT[,is.na(AHS.Vehicle.ID) | AHS.Vehicle.ID == ""]))
  data.table::setkey(fleetDT, 'AHS.Vehicle.ID')
  
  # Existing IDS Vehicles
  idsDT = data.table::fread(here::here('../data/raw/Vehicle_List_20161020.csv'), check.names = TRUE)
  stopifnot(!any(idsDT[,is.na(EHS.NUMBER) | EHS.NUMBER == ""]))
  data.table::setkey(idsDT, 'EHS.NUMBER')
  
  # CSD records of units
  shiftsDT = data.table::fread(here::here('../data/raw/tbl_ShiftInventory_20190320.csv'), check.names = TRUE)
  data.table::setkey(shiftsDT, 'Shift_No')
  
  # Historical record of all vehicle IDs for units logged in in the last 2 years
  unhiDT = data.table::fread(here::here('../data/raw/qry_UN_HI_CARID_full_20190318.csv'), check.names = TRUE)
  data.table::setkey(unhiDT, 'CARID')
  
  return(list(fleetDT=fleetDT, idsDT=idsDT, unhiDT=unhiDT, shiftsDT=shiftsDT))
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

new_vehicles <- function(fleetDT, idsDT) {
  newDT = fleetDT[!fleetDT[,AHS.Vehicle.ID] %in% idsDT[,Name]]
}


# Load
unpack[fleetDT, idsDT, unhiDT, shiftsDT] <- load()


# Filter historical vehicle lists based on vehicle type (no planes, trains or bikes)
unhiDT = filter_nonvehicles(unhiDT)

# Remove decommissioned vehicles from fleet
fleetDT = filter_decommissioned(fleetDT)

# Identify contractor/dd unit names
unpack[contrDT, directDT] = filter_by_service(shiftsDT)
directDT = filter_nonvehicles(directDT, quote(Unit_Name))

# Any units not LN in last 2 years
nolnDT = directDT[!directDT[,Unit_Name] %in% unhiDT[,UNID]]

# Do they map if you use the alternate?
nolnDT[str_detect(Unit_Name, '(-\\d)A(\\d+)$'),alt:=str_replace(Unit_Name, '(-\\d)A(\\d+)$', '\\1B\\2')]
nolnDT[str_detect(Unit_Name, '(-\\d)B(\\d+)$'),alt:=str_replace(Unit_Name, '(-\\d)B(\\d+)$', '\\1A\\2')]
nolnDT2 = nolnDT[!nolnDT[,alt] %in% unhiDT[,UNID]]

loginfo(paste0('The following direct delivery units (and associated A/B alternate) from tbl_ShiftInventory have not logged in\n  (no record in UN_HI in last 2 years):\n', 
               paste(sort(nolnDT2[,Unit_Name]), collapse='\n')))

# Map vehicles to units for direct and contractor vehicles
setkey(unhiDT, 'UNID')
setkey(directDT, 'Unit_Name')

unhiDT2 = unhiDT[directDT]
unhiDT2 = unhiDT2[!is.na(CARID)]
unhiDT2[,DGroup:=str_extract(UNID, '^[:upper:]{4}')]

setkey(contrDT, 'Unit_Name')
unhiDT3 = unhiDT[contrDT]
unhiDT3 = unhiDT3[!is.na(CARID)]
contrDT = unhiDT3[,.(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), by=CARID]

# Identify problem units/vehicle combos

# Which vehicles are not frequently used in aggregate
tmpDT1 = unhiDT2[,.(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), 
                 by=CARID][N < 5][order(as.numeric(CARID))]
tmp = kable(tmpDT1, format='markdown')
loginfo(paste0('Infrequently used vehicles:\n', paste0(tmp, collapse="\n")))

# Which vehicles are also being reported as being used by contractor unit
tmpDT2 = unhiDT2[CARID %in% contrDT[,CARID],
                 .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')), 
                 by=CARID][order(CARID, -N)]
tmpDT2 = merge(tmpDT2, contrDT, by='CARID', all.x=TRUE, suffixes = c('.d','.c'))
tmp = kable(tmpDT2, format='markdown')
loginfo(paste0('Vehicles appearing in Contractor Services:\n', paste0(tmp, collapse="\n")))

unhiDT4 = unhiDT2[!CARID %in% tmpDT2[N.c>N.d,CARID]]
dd.vehicles = sort(unique(unhiDT4[,CARID]))
loginfo(paste0('Direct Delivery vehicles:\n', paste0(dd.vehicles, collapse="\n")))

# Which vehicles do we not have config information for (documented)
undoc.vehicles = dd.vehicles[!dd.vehicles %in% idsDT[,EHS.NUMBER]]

loginfo(paste0('Vehicles with no config information:\n', paste0(undoc.vehicles, collapse="\n")))

# Which vehicles are in fleet database
unkn.vehicles = undoc.vehicles[!undoc.vehicles %in% fleetDT[,AHS.Vehicle.ID]]
fleet.vehicles = undoc.vehicles[undoc.vehicles %in% fleetDT[,AHS.Vehicle.ID]]


tmpDT3 = unhiDT4[CARID %in% unkn.vehicles, 
                 .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';')),
                 by=CARID]
tmp = kable(tmpDT3, format='markdown')
loginfo(paste0('Vehicles not found in fleet database:\n', paste0(tmp, collapse="\n")))

# Check information on undocumented vehicles found in fleet (vehicles with no prior config information)
fleetDT = fleetDT[AHS.Vehicle.ID %in% fleet.vehicles]
fleetDT[,.N,by=Classification]

# Tackle by Vehicle Classifcation to determine gaps in information (needed information changes based on vehicle type)

# Most ambulances (with exception of rare specialty units) should have the same configuration
data.cols = c('IsActive', 'VehicleType', 'InServicedate', 'StockLevel', 'Division', 'StretcherConfig', 'StretcherType',
              'PtCompSize','SeatingConfig', 'SeatingConfig2')
ambDT = fleetDT[Classification == "AMB-Primary"]

missing.data = apply(ambDT[,data.cols, with=FALSE], 1, function(r) any(is.na(r)) | any(r == ""))


is_stardard_amb_config <- function(arow) {
  
  r = c(arow["IsActive"] == TRUE,
        arow["VehicleType"] == "Ambulance Type III",
        arow["StockLevel"] == "ALS",
        arow["StretcherConfig"] == "Power Load",
        grepl('stryker.+power.+pro.+xt', arow["StretcherType"], ignore.case = TRUE),
        arow["PtCompSize"] == "164",
        arow["SeatingConfig"] == "Regular - 3",
        arow["SeatingConfig2"] == "Folding Seats - 2"
        )
  
  return(all(r))
}
sum(apply(ambDT[!missing.data, data.cols, with=FALSE], 1, is_stardard_amb_config))
