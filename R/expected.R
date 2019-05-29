# SUV vehicles

library(data.table)
library(lubridate)
library(here)
library(stringr)
library(logging)


logReset()
basicConfig(level='DEBUG')
lfn = here::here('../data/interim/expected.log')
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
  
  # Unit Types from CSD
  vehtypDT = data.table::fread(here::here('../data/raw/tbl_VehicleType_20190529.csv'), check.names = TRUE)
  vehtypDT = vehtypDT[Active == 1, .(Vehicle,VehicleType)]
  data.table::setkey(unhiDT, 'CARID')
  
  return(list(fleetDT=fleetDT, oldDT=oldDT, unhiDT=unhiDT, shiftsDT=shiftsDT, vehicDT=vehicDT, vehtypDT=vehtypDT))
}


filter_nonvehicle_carids <- function(vDT, Vcol=quote(CARID)) {
  vDT = vDT[grepl('^\\d+$', CARID)]
  return(vDT)
}

filter_nonvehicles <- function(uDT, Vcol=quote(UNID)) {
  uDT = uDT[!grepl('^XV', eval(Vcol))]
  uDT = uDT[!grepl('^FRD', eval(Vcol))]
  uDT = uDT[!grepl('-\\d[XYVUROKJIGFZ]\\d+$', eval(Vcol), perl=TRUE)]
  uDT = uDT[!grepl('^[[:upper:]]{4}\\d$', eval(Vcol), perl=TRUE)]
  uDT = uDT[!grepl('^[[:upper:]]{3}-', eval(Vcol), perl=TRUE)]
  
  return(uDT)
}


output_staging_table <- function(dt) {
  mydt = dt[,.(Vehicle,VehicleType)]
  mydt[,`:=`(Active="true", EffectiveStart=Sys.Date())]
  data.table::fwrite(mydt, here::here('../data/interim/initial_suv_staging_table.csv'))
}


# Load
unpack[fleetDT, oldDT, unhiDT, shiftsDT, vehicDT, vehtypDT] <- load()

# Select relevant shifts
shiftsDT = shiftsDT[Active.Inactive == "Active"]
shiftsDT = shiftsDT[CAD.Unit.Type %in% c("ALS","BLS","PRU","ENAT","BNAT","EMR","HELI","WING","FLIGHT","ALSr")]
shiftsDT[,generic:=str_replace(Unit_Name, "(\\d)[AB](\\d+)$", "\\1_\\2")]

# Filter unit history to look at recent vehicle usage and omit short shifts
dt = Sys.Date() - lubridate::days(180)
unhiDT = unhiDT[as.Date(CDTS2) > dt & TIME_ACTIVE >= 3*60*60]

# Merge vehicle history with shift data
expDT = merge(unhiDT, shiftsDT, by.x='UNID', by.y='Unit_Name', all=FALSE)
expDT = merge(expDT, vehtypDT, by.x='CARID', by.y='Vehicle', all.x=TRUE, all.y=FALSE)

# Assign Expected Vehicle Type for each generic, as well as available Default Vehicle
# Default Vehicle just needs to be same type, its not critical if its the main vehicle for unit

default_vehicles = c()

# When no obvious type, use this ranking to pick the lowest possible type
vt_rank = c(
  NA, 
  "AA002S", 
  "AA002N",
  "AA004N",
  "WA002N",
  "SC002N",
  "SW002N",
  "SW004N",
  "SS002", 
  "SS003",
  "SS005",
  "SS006",
  "DS005",
  "SS013",
  "SC014",
  "SS015",
  "SC016",
  "DS014",
  "DS015",
  "SC114",
  "SC115"       
)
expected_unit <- function(dt) {
  
  #print(dt)
  
  tot = nrow(dt)
  counts = dt[, .(.N, p=.N/tot), by=VehicleType]
  res = counts[which.max(p), .(N, p, VehicleType)]
  
  if(res[,p]>.6) {
    # Pick unit used most often
    typ = res[,VehicleType]
    
  } else {
    # No clear winner
    # Pick lowest capacity vehicle type
    
    if(any(counts[,N] > 1)) {
      # If possible, omit Vehicles that are only used once
      vt = counts[N > 1, VehicleType]
      
    } else {
      vt = counts[N > 1, VehicleType]
    }
    
    minvt = vt[which.min(match(vt, vt_rank))]
    
    logdebug(paste('Unit:', dt[1, UNID], 'does not have majority unit type. Falling back to:', minvt))
    logdebug('START TYPE COUNTS for UNIT\n')
    for(i in 1:nrow(counts)) {
      logdebug(paste('\t',counts[i, VehicleType], ':', counts[i, N]))
    }
    logdebug('END TYPE COUNTS')
    
    typ = minvt
  }
  
  # Pick available default vehicle with matching vehicle type
  vehicles = dt[VehicleType == typ, .N, by=CARID]
  setorder(vehicles, -N)
  
  if(is.na(typ)) {
    dv = NA
  } else {
    notavail = vehicles[,CARID] %in% default_vehicles
    if(all(notavail)) {
      # Use duplicate vehicle
      dv = vehicles[which.max(N), CARID]
      
    } else {
      dv = vehicles[which(notavail==FALSE)[1], CARID]
      default_vehicles = c(default_vehicles, dv)
    }
  }
  
  #print(paste(typ, dv))
  return(list(as.character(typ), as.character(dv)))

}
expDT2 = expDT[, c("ExpectedUnitType", "DefaultVehicle"):=expected_unit(.SD), by=generic]
expDT2 = unique(expDT2, by=c("UNID", "ExpectedUnitType", "DefaultVehicle", "generic"))
expDT2 = expDT2[, .(Unit=min(UNID), ExpectedUnitType, DefaultVehicle), by=generic]
