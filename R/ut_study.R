# Frequency of unit type changes for a given unit

library(data.table)
library(lubridate)
library(here)
library(readxl)
library(stringr)
library(logging)
library(ggplot2)


logReset()
basicConfig(level='DEBUG')
lfn = here::here('../data/interim/utstudy.log')
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
  vehtypDT = data.table::fread(here::here('../data/raw/tbl_VehicleType_20190827.csv'), check.names = TRUE)
  vehtypDT = vehtypDT[Active == 1, .(Vehicle,VehicleType)]
  data.table::setkey(unhiDT, 'CARID')
  
  return(list(fleetDT=fleetDT, oldDT=oldDT, unhiDT=unhiDT, shiftsDT=shiftsDT, vehicDT=vehicDT, vehtypDT=vehtypDT))
}

vt_rank = c(
  NA, 
  "AA002S", 
  "AA002N",
  "AA004N",
  "WA002N",
  "SC002N",
  "SW002N",
  "SW004N",
  "SS001",
  "SS002",
  "SS004",
  "SS005",
  "SS012",
  "SC013",
  "SS014",
  "SC015",
  "SC113",
  "SC114",
  "DS004",
  "DS013",
  "DS014",
  "DS016"
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
      # Omit Vehicles that are only used once
      vt = counts[N > 1, VehicleType]
      matches = match(vt, vt_rank)
      stopifnot(any(!is.na(matches)))
      minvt = vt[which.min(matches)]
      
      logdebug(paste('Unit:', dt[1, UNID], 'does not have majority unit type. Falling back to:', minvt))
      logdebug('START TYPE COUNTS for UNIT\n')
      for(i in 1:nrow(counts)) {
        logdebug(paste('\t',counts[i, VehicleType], ':', counts[i, N]))
      }
      logdebug('END TYPE COUNTS')
      
    } else {
      logdebug(paste('Unit:', dt[1, UNID], ' has not used any vehicle type more than twice'))
      minvt = NA
    }
    
    typ = minvt
  }
  
  return(as.character(typ))
}

# Default VT
defaultDT = data.table(
  VT=c('SC113', 'SC113', 'AA002N', 'SC113', 'SC113'),
  UT=c('A','B','T','W','C')
)

# Load
unpack[fleetDT, oldDT, unhiDT, shiftsDT, vehicDT, vehtypDT] <- load()

# Select relevant ground unit shifts
# Note: aircraft handled separately since their vehicle use is not logged the same way
shiftsDT = shiftsDT[Active.Inactive == "Active"]
aircraftDT = data.table::copy(shiftsDT)
shiftsDT = shiftsDT[CAD.Unit.Type %in% c("ALS","BLS","ENAT","BNAT","EMR","WING","ALSr")]
shiftsDT[,generic:=str_replace(Unit_Name, "-(\\d)[AB](\\d+)$", "-\\1_\\2")]
aircraftDT = aircraftDT[CAD.Unit.Type %in% c("HELI","FLIGHT")]
aircraftDT = aircraftDT[,generic:=Unit_Name]

# Merge vehicle history with shift data
expDT = merge(unhiDT, shiftsDT, by.x='UNID', by.y='Unit_Name', all=FALSE)
expDT = merge(expDT, vehtypDT, by.x='CARID', by.y='Vehicle', all.x=TRUE, all.y=FALSE)
expDT = expDT[!is.na(VehicleType)]
expDT = expDT[,.(date=as.Date(CDTS2),UNID,generic,CARID,VehicleType,District)]
expDT[,UT:=substr(UNID,7,7)]


# Vehicle Type Counts
dt1 = expDT[,.N, by=VehicleType]
dt1 %>% ggplot(aes(x=reorder(VehicleType, -N), y=N)) + 
  geom_bar(stat="identity") + 
  labs(title="Vehicle type frequency", y="Number of shifts", x="Vehicle Type") +
  theme(axis.text.x = element_text(hjust=1, angle = 45))

# Histogram of number of types used per vehicle
expDT[,.(N=uniqueN(VehicleType)),by=generic] %>% ggplot(aes(x=N)) +
  geom_histogram(binwidth = 1) +
  labs(title="VehicleType Histogram", subtitle="Nmber of different Vehicle types used by unit", y="Frequency", x="Vehicle type count")

# Transitions
# Counts
dt2 = expDT[order(date),.(date, curr=VehicleType, prev=shift(VehicleType, 1, 'lag')),by=.(generic)]
dt2 = dt2[prev != 'lag']
m1 = table(dt2$curr, dt2$prev)
dt2 = dt2[,.N,by=.(curr,prev)]
dt2[,N2:=N]
dt2[N2>10000,N2:=10000]

dt2 %>% ggplot(aes(x=curr, y=prev, fill=N)) + 
  geom_tile() +
  labs(title="Transition frequency", subtitle="Vehicle types in consecutive shifts", y="Previous vehicle type", x="Current vehicle type") +
  theme(axis.text.x = element_text(hjust=1, angle = 45))

# Transitions
# Proportions
m2 = round(m1 / rowSums(m1),3)*100
dt2 = dt2[,.(prev,N,tot=sum(N)),by=curr]
dt2[,`:=`(p=N/tot)]

dt2 %>% ggplot(aes(x=curr, y=prev, fill=p)) + 
  geom_tile() +
  labs(title="Transition likelihood", subtitle="Vehicle types in consecutive shifts", y="Previous vehicle type", x="Current vehicle type") +
  theme(axis.text.x = element_text(hjust=1, angle = 45))

# Number of changes
dt2[curr != prev, .(total_changes=sum(N))]
dt2[curr != prev, sum(N)]/dt2[,sum(N)] * 100

# Default approach
dt3 = merge(expDT, defaultDT, by='UT')

# Set VehicleTypes per DGROUP


# Update vehicle type each month
expDT[,`:=`(ym=paste(year(date),month(date),sep='-'))]


expDT2 = expDT[, "ExpectedUnitType":=expected_unit(.SD), by=generic]





