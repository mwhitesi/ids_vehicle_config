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
  vehtypDT = data.table::fread(here::here('../data/raw/tbl_VehicleType_20190531.csv'), check.names = TRUE)
  vehtypDT = vehtypDT[Active == 1, .(Vehicle,VehicleType)]
  data.table::setkey(unhiDT, 'CARID')
  
  return(list(fleetDT=fleetDT, oldDT=oldDT, unhiDT=unhiDT, shiftsDT=shiftsDT, vehicDT=vehicDT, vehtypDT=vehtypDT))
}


output_expected_table <- function(dt) {
  mydt = dt[!is.na(ExpectedUnitType)]
  mydt[,`:=`(ExpectedUnitTypeKey=str_c('UT_', ExpectedUnitType))]
  mydt = mydt[,.(generic, ExpectedUnitType, DefaultVehicle, ExpectedUnitTypeKey)]
  data.table::fwrite(mydt, here::here('../data/interim/tbl_Vehicle_expected.csv'))
}


# Load
unpack[fleetDT, oldDT, unhiDT, shiftsDT, vehicDT, vehtypDT] <- load()

# Select relevant shifts
shiftsDT = shiftsDT[Active.Inactive == "Active"]
shiftsDT = shiftsDT[CAD.Unit.Type %in% c("ALS","BLS","PRU","ENAT","BNAT","EMR","HELI","WING","FLIGHT","ALSr")]
shiftsDT[,generic:=str_replace(Unit_Name, "-(\\d)[AB](\\d+)$", "-\\1_\\2")]

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
  "SS013",
  "SC014",
  "SS015",
  "SC016",
  "SC114",
  "SC115",
  "DS005",
  "DS014",
  "DS015"
)
expected_unit <- function(dt) {
  
  # Keep the 30 most recent
  shift.times = sort(dt[,CDTS2], decreasing = TRUE)
  if(length(shift.times) < 30) {
    cutoff = shift.times[length(shift.times)]
  } else {
    cutoff = shift.times[30]
  }
  dt = dt[CDTS2 >= cutoff]
  
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
      vt = counts[, VehicleType]
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
    if(any(!notavail)) {
      dv = vehicles[which(notavail==FALSE)[1], CARID]
      default_vehicles <<- c(default_vehicles, dv)
    } else {
      dv = NA
    }
  }
  
  return(list(as.character(typ), as.character(dv)))

}
expDT2 = expDT[, c("ExpectedUnitType", "DefaultVehicle"):=expected_unit(.SD), by=generic]
expDT2 = unique(expDT2, by=c("ExpectedUnitType", "DefaultVehicle", "generic"))
expDT2 = expDT2[]

# Assign DefaultVehicle for remaining
for(i in 1:nrow(expDT2)) {
  dv = expDT2[i,DefaultVehicle]

  if(is.na(dv)) {
    reqd_typ = expDT2[i,ExpectedUnitType]
    if(!is.na(reqd_typ)) {
      print(paste('Looking for',reqd_typ))
      dv = vehtypDT[VehicleType == reqd_typ & !Vehicle %in% default_vehicles, Vehicle]
      print(dv)
      
      if(length(dv) > 0 & any(!is.na(dv))) {
        # Pick an available vehicle with matching type
        dv = dv[1]
        default_vehicles <<- c(default_vehicles, dv)
        expDT2[i,DefaultVehicle:=dv]
      } else {
        # Nothing left, use a duplicate
        this.vehicles = expDT[generic == expDT2[i,generic], .N, CARID]
        dv = this.vehicles[which.max(N), CARID]
        expDT2[i,DefaultVehicle:=dv]
      }
    }
  }
}

output_expected_table(expDT2)

  

