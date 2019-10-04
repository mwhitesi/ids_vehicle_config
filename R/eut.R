# Generate expected unit type
library(stringr)
library(logging)
library(here)
library(data.table)

library(lubridate)

# logging::logReset()
# logging::basicConfig(level='INFO')
# lfn = here::here('../data/interim/eut.log')
# if (file.exists(lfn)) 
#   file.remove(lfn)
# logging::addHandler(writeToFile, file=lfn, level='DEBUG')

mylist <- list(


mylist[['Get_VehHist' = function() {
  # Get most recent 90 shifts of data for each unit looking back at most 1 year
  z_src = 'ARCH_ICAD'
  min_sft_sec = 3 * 3600
  max_date = '20181001'
  max_rec = 90
  z_sql = paste0("WITH [Z1] AS ( ",
                 "SELECT H.[UNID], H.[CARID], H.[CDTS2], [UNIT_STATUS], [TIME_ACTIVE], ",
                 "ROW_NUMBER() OVER (PARTITION BY H.[UNID] ORDER BY H.[UNID], H.[CDTS2] DESC) AS [ROW__] ",
                 "FROM [AHS_ARCH].[dbo].[UN_HI] H, [AHS_ARCH].[dbo].[UNIT_WKLOAD] W ",
                 "WHERE CARID IS NOT NULL AND ",
                 "LEFT(H.CDTS,8) > '",max_date,"' AND ", 
                 "UNIT_STATUS = 'LO' AND ",
                 "H.CDTS = W.CDTS AND ",
                 "H.UNID = W.UNID AND ",
                 "TIME_ACTIVE >= ",min_sft_sec,
                 " ), ",
                 "[Z0] AS (SELECT * FROM [Z1] WHERE [ROW__] <= ",max_rec,") ",
                 "SELECT * FROM [Z0]")
  
  z_dat <- UTILS$DatSrc_ConGetDis(z_src, z_sql)
  
  return(z_dat)
}

Get_VehTyp <- function() {
  
  z_src = 'PRDa_LIDS'
  z_sql = paste0("SELECT [Name], [ExternalKey], [VehicleTypeExternalKey] ",
    "FROM [LogisCAD_DwhStd].[dbo].[DwhVehicle] ",
    "WHERE [DeletedStatus] = 0")
  
  z_dat <- UTILS$DatSrc_ConGetDis(z_src, z_sql)
  
  return(z_dat)
}
  
Get_Shifts <- function() {
  
  # Retrieve
  z_src = 'CSD'
  z_sql = paste0("SELECT * ",
    "FROM tbl_ShiftInventory AS SI ",
    "WHERE SI.[Active/Inactive]='Active' ",
    "ORDER BY SI.Unit_Name;")
  
  z_dat <- UTILS$DatSrc_ConGetDis(z_src, z_sql)
  
  return(z_dat)
}

Filter_Shifts <- function(df1) {
  
  #WHERE (((SI.Type) Not In ('Transition Room')) AND ((SI.[CAD Unit Type]) Not In ('DISP','ITTEST')) AND ((SI.[Active/Inactive])='Active') AND ((STN.Status) Not In ('Inactive')))
  
  ut = c("ALS","BLS","PRU","ENAT","BNAT","EMR","HELI","WING","FLIGHT","ALSr", "COMP", "SUPER", "DELTA", "ECHO", "MIKE")
  df1[ (Unit_Name %in% c("PICT-5A1","PICT-5B1") & Days == "Weekend") | (!(Unit_Name %in% c("PICT-5A1","PICT-5B1")) & `CAD Unit Type` %in% ut)]
  
  return(df1)
}

Get_ExpVT <- function() {
  
  # Retrieve
  z_src = 'CSD'
  z_sql = paste0("SELECT [Grouped_Unit_Name], [LUT_Key], [Expected_Vehicle_Type_Key] ",
                 "FROM tbl_VehicleType_expected")
  
  z_dat <- UTILS$DatSrc_ConGetDis(z_src, z_sql)
  
  return(z_dat)
}

Calc_ExpVT <- function(testset_date_cutoff=NA, hDT=NULL, lookback=7, prop=.5) {
  
  # Retrieve data
  sDT = Get_Shifts()
  sDT = Filter_Shifts(sDT)
  vDT = Get_VehTyp()
  if(is.null(hDT)) hDT = Get_VehHist()
  
  # Join shift and historical data
  eDT = merge(hDT, sDT, by.x='UNID', by.y='Unit_Name', all.x=FALSE, all.y=TRUE)
  eDT = merge(eDT, vDT, by.x='CARID', by.y='Name', all.x=TRUE, all.y=FALSE)
  
  if(!is.na(testset_date_cutoff)) {
    eDT[,dt:=as.POSIXct(CDTS2, format="%Y-%m-%d %H:%M:%S", tz="UTC")]
    ts_date = as.POSIXct(testset_date_cutoff, format="%Y-%m-%d", tz="UTC")
    testDT = eDT[dt > ts_date]
    eDT = eDT[dt <= ts_date]
    
    # Add back units that do not exist before test date as a "new" unit (i.e. all historical values NA)
    nDT = sDT[!Unit_Name %in% eDT[,UNID]]
    nDT[,UNID:=Unit_Name]
    nDT[,Unit_Name:=NULL]
    eDT = data.table::rbindlist(list(eDT, nDT), use.names=TRUE, fill=TRUE)
    
  }
  
  # Assign a default type to use for new units or old/obsolete units that we have no login history
  # Base it first on vehicle ID, if it exists
  # Secondly on the unit type
  eDT[,veh:=ifelse(suppressWarnings(is.na(as.integer(CARID))), -1, suppressWarnings(as.integer(CARID)))]
  eDT[,cad_ut:=`CAD Unit Type`]
  
  eDT[, default_ut:= c("UT_AA002N", "UT_SC002", "UT_AA002S", "UT_GhostVehicle", 
                       "UT_AA002N", "UT_SC002", "UT_AA002S", "UT_GhostVehicle", 
                       "UT_SS002A.450", "UT_SS002H.250", "UT_GhostVehicle", "Undefined")[
    apply(cbind(
      ((veh > 4999 & veh < 9000) | (veh > 19999 & veh < 40000)), # NAT
      (veh > 999 & veh < 5000), # Ambulance
      ((veh > 39999 & veh < 50000) | (veh > 9999 & veh < 19999)), # SUV
      ((veh > 899 & veh < 1000) | (veh > 8999 & veh < 10000)), # MDT
      (cad_ut %in% c('BNAT', 'ENAT')), # NAT
      (cad_ut %in% c('ALS', 'BLS', 'BLSr', 'ALSr', 'WING')), # Ambulance
      (cad_ut %in% c('DELTA', 'COMP', 'SUPER', 'ECHO', 'MIKE', 'PRU', 'FRU', 'BPRU', 'ITTEST', 'EMR')), # SUV
      (cad_ut %in% c('DISP', 'SEGWAY', 'ATV', 'FOOT', 'BIKE', 'CART', 'TRAIN', 'BUS', 'MASS', 'HOSP')), # MDT/Non-Transport 
      (cad_ut == "FLIGHT"),
      (cad_ut == "HELI"),
      ((cad_ut == "OTHER") | (cad_ut == "Other")),
      TRUE), 1, which.max)]
    ]
  
  # Assign a resource specific type based on history
  eDT[, grouped_unit:=ifelse(stringr::str_detect(UNID,"^[A-Z]{4}\\-\\d[AB]\\d+$"), 
                             paste0(stringr::str_sub(UNID,1,6),'_',stringr::str_sub(UNID,8,100)), UNID)]
  
  # When multiple candidate types exist, select type with lowest capabilities
  type_ranking <- function(typ) {
    typ = typ[!is.na(typ)]
    
    if(length(typ) == 0) return(NA)
    
    df = data.table(typ=typ, c=0, s=0, w=0, a=0, d=0)
    df[, typ:=stringr::str_replace(typ, '^UT_Air_', '')]
    df[, typ:=stringr::str_replace(typ, '^UT_', '')]
    
    # Increment types scores
    df[stringr::str_sub(typ,6,6)=="S", c:=1] # SUV
    df[stringr::str_sub(typ,6,6)=="N", c:=2] # NAT
    df[stringr::str_sub(typ,6,6)=="", c:=3] # AMB
    df[stringr::str_sub(typ,6,6)=="H", c:=4] # HELI
    df[stringr::str_sub(typ,6,6)=="H", c:=5] # FLIGHT
    
    # Increment stretcher types scores
    df[stringr::str_sub(typ,1,4)=="SS00", s:=1]
    df[stringr::str_sub(typ,1,4)=="SC00", s:=2]
    df[stringr::str_sub(typ,1,4)=="DS00", s:=3]
    df[stringr::str_sub(typ,1,4)=="SW00", s:=3.5]
    df[stringr::str_sub(typ,1,4)=="SS01", s:=4]
    df[stringr::str_sub(typ,1,4)=="SC01", s:=5]
    df[stringr::str_sub(typ,1,4)=="DS01", s:=6]
    df[stringr::str_sub(typ,1,4)=="SW01", s:=6.5]
    df[stringr::str_sub(typ,1,4)=="SS10", s:=7]
    df[stringr::str_sub(typ,1,4)=="SC10", s:=8]
    df[stringr::str_sub(typ,1,4)=="DS10", s:=9]
    df[stringr::str_sub(typ,1,4)=="SW10", s:=9.5]
    df[stringr::str_sub(typ,1,4)=="SS11", s:=10]
    df[stringr::str_sub(typ,1,4)=="SC11", s:=11]
    df[stringr::str_sub(typ,1,4)=="DS11", s:=12]
    df[stringr::str_sub(typ,1,4)=="SW11", s:=12.5]
    
    # Increment wheelchairs
    stopifnot(all(df[stringr::str_sub(typ,1,2)=="SW" | stringr::str_sub(typ,1,2)=="WA", stringr::str_sub(typ,7,100) != ""]))
    df[stringr::str_sub(typ,1,2)=="SW" | stringr::str_sub(typ,1,2)=="WA", w:=as.numeric(str_sub(typ,7,100)) ]
    
    # Increment all capacity scores
    df[,a:=as.numeric(stringr::str_sub(typ,5,5))]
    
    # Increment air speed scores
    stopifnot(all(df[stringr::str_sub(typ,6,6)=="H" | stringr::str_sub(typ,6,6)=="A", stringr::str_sub(typ,7,100) != ""]))
    df[stringr::str_sub(typ,6,6)=="H" | stringr::str_sub(typ,6,6)=="A", d:=as.numeric(stringr::str_sub(typ,7,100)) ]
    
    # Find row with lowest score using relative importance as follows:
    setorder(df, c('c', 's', 'w', 'a', 'd', 'typ'))
    
    return(df[!is.na(typ),typ][1])
  }
  
  # Identify expected 
  expected_unit <- function(dt, lookback, prop) {
    
    default_veh = as.character(dt[1,default_ut])
    logmsg = paste(dt[1,UNID], dt[1, CARID],"-")
    
    # Keep the X most recent
    shift.times = sort(dt[,CDTS2], decreasing = TRUE)
    if(length(shift.times) < lookback) {
      cutoff = shift.times[length(shift.times)]
    } else {
      cutoff = shift.times[lookback]
    }
    dt = dt[CDTS2 >= cutoff & !is.na(VehicleTypeExternalKey)]
    
    tot = nrow(dt)
    counts = dt[, .(.N, p=.N/tot), by=VehicleTypeExternalKey]
    res = counts[which.max(p), .(N, p, VehicleTypeExternalKey)]
    
    if(tot == 0) {
      logging::logdebug(paste(logmsg, 'no history. Selecting default:',default_veh))
      return(default_veh)
    }
    
    if(res[,p]>prop) {
      # Pick unit used most often
      typ = res[,VehicleTypeExternalKey]
      logmsg = paste(logmsg, 'consistent type used >',prop,'times in last',lookback,'shifts: ',typ)
      
    } else {
      # No clear winner
      # Pick lowest capacity vehicle type
      
      if(any(counts[,N] > 1)) {
        # Omit Vehicles that are only used once
        vt = counts[N > 1, VehicleTypeExternalKey]
        
        minvt = type_ranking(vt)
        logmsg = paste(logmsg, 'inconsistent type, selecting minimum:',minvt,'from',paste(vt,collapse=', '))
        
      } else {
        
        minvt = default_veh
        logmsg = paste(logmsg, 'no vehicles used >1 times in last',lookback,'shifts. Selecting default:',default_veh)
      }
      
      typ = minvt
    }

    logging::logdebug(logmsg)
    return(as.character(typ))
  }
  
  eDT[, c("ExpectedVehicleTypeKey"):=expected_unit(.SD, lookback=lookback, prop=prop), by=grouped_unit]
  eDT[,ExternalKey:=paste0("LUT_",UNID)]
  eDT2 = unique(eDT, by=c("ExpectedVehicleTypeKey", "ExternalKey", "grouped_unit", "UNID"))[,.(ExternalKey, ExpectedVehicleTypeKey, grouped_unit, UNID)]
  
  if(any(eDT2[,ExpectedVehicleTypeKey == "Undefined"])) {
    warning(paste('Unable to identify default vehicle type for record(s):', 
                  paste(eDT2[ExpectedVehicleTypeKey == "Undefined", ExternalKey], collapse = ', ')))
    eDT2=eDT2[ExpectedVehicleTypeKey != "Undefined"]
  }
  
  if(!is.na(testset_date_cutoff)) {
    # Compute performance
    testDT = merge(testDT, eDT2, by = 'UNID', all.x = TRUE)
    
    # New units since test cutoff need a default for this to reflect "reality"
    
    
    tot = nrow(testDT)
    logging::logdebug(paste0('-- Test Results --'))
    m = sum(testDT[!is.na(VehicleTypeExternalKey) & !is.na(ExpectedVehicleTypeKey), VehicleTypeExternalKey == ExpectedVehicleTypeKey])
    mm = sum(testDT[!is.na(VehicleTypeExternalKey) & !is.na(ExpectedVehicleTypeKey), VehicleTypeExternalKey != ExpectedVehicleTypeKey])
    uv = sum(testDT[, is.na(VehicleTypeExternalKey)])
    ut = sum(testDT[, is.na(ExpectedVehicleTypeKey)])
    logging::logdebug(paste0('Matches: ', m,' (', round(m/tot*100, 2),')'))
    logging::logdebug(paste0('Mismatches: ', mm,' (', round(mm/tot*100, 2),')'))
    logging::logdebug(paste0('Unknown vehicle: ', uv,' (', round(uv/tot*100, 2),')'))
    logging::logdebug(paste0('Undefined expected vehicle type: ', ut,' (', round(ut/tot*100, 2),')'))
  } else {
    testDT = NULL
  }
  
  return(list(eDT2, testDT))
}

Upd_ExpVT <- function() {
  
  # Compute Expected Vehicle Type
  rs = Calc_ExpVT()
  eDT = rs[[1]]
  # Retrieve current values
  vDT = Get_ExpVT()
  
  mDT = merge(eDT, vDT, by.x = "ExternalKey", by.y="LUT_Key", all.x=TRUE)
  
  z_con = UTILS$DatSrc_con('CSD')
  
  # Insert new records
  # Couldn't get multi-insert or parameterized queries to work
  # Only way is to insert 1 record at a time as sql statement string 
  nDT = mDT[is.na(Expected_Vehicle_Type_Key)]
  nr = nrow(nDT)
  logging::logdebug(paste0("Inserting ",nr," new records"))
  if(nr > 0) {
    for(i in 1:nr) {
      z_sql = paste0("INSERT INTO tbl_VehicleType_expected (Grouped_Unit_Name, LUT_Key, Expected_Vehicle_Type_Key) VALUES ('",
                     paste(nDT[i, .(grouped_unit, ExternalKey, ExpectedVehicleTypeKey)], collapse="','"), "');")
      
      z_i = DatSrc_exec(z_con, z_sql)
      
      if(z_i == 1) {
        logging::logdebug(paste0('Inserted record ',nDT[i, ExternalKey]))
      } else {
        logging::logdebug(paste0('Insertion failed for record ',nDT[i, ExternalKey]))
      }
    }
  }

  # Updated changed records
  uDT = mDT[!is.na(Expected_Vehicle_Type_Key) & Expected_Vehicle_Type_Key != ExpectedVehicleTypeKey]
  nr = nrow(uDT)
  logging::logdebug(paste0("Updating ",nr," records"))
  if(nr > 0) {
    for(i in 1:nr) {
      z_sql = paste0("UPDATE tbl_VehicleType_expected SET [Expected_Vehicle_Type_Key] = '", uDT[i,ExpectedVehicleTypeKey],
                     "' WHERE [LUT_Key] = '",uDT[i,ExternalKey],"';")
      
      z_i = DatSrc_exec(z_con, z_sql)
      
      if(z_i == 1) {
        logging::logdebug(paste0('Updated record ',nDT[i, ExternalKey], ' with new VT: ', nDT[i, ExpectedVehicleTypeKey]))
      } else {
        logging::logdebug(paste0('Update failed for record ',nDT[i, ExternalKey]))
      }
    }
  }
  
  UTILS$DatSrc_dis(z_con)
}


Test_ExpVT <- function() {
  hDT = Get_VehHist()
  
  test_params = list(c(14, .5), c(28, .5), c(7, .5), c(1, .5), 
                     c(14, .25), c(28, .25), c(7, .25),
                     c(14, .75), c(28, .75), c(7, .75))
  for(j in 1:length(test_params)) {
    
    logging::loginfo(paste0("\n\r-- Test Scenario --\n\r"))
    lb = test_params[[j]][1]
    prp = test_params[[j]][2]
    logging::loginfo(paste0("Lookback window: ", lb))
    logging::loginfo(paste0("Assignment threshold: ", prp))
  
    rs = Calc_ExpVT(testset_date_cutoff = Sys.Date() - 30, hDT = hDT, lookback = lb, prop = prp)
    testDT = rs[[2]]
    tot = nrow(testDT)
   
    m = sum(testDT[!is.na(VehicleTypeExternalKey) & !is.na(ExpectedVehicleTypeKey), VehicleTypeExternalKey == ExpectedVehicleTypeKey])
    mm = sum(testDT[!is.na(VehicleTypeExternalKey) & !is.na(ExpectedVehicleTypeKey), VehicleTypeExternalKey != ExpectedVehicleTypeKey])
    uv = sum(testDT[, is.na(VehicleTypeExternalKey)])
    ut = sum(testDT[, is.na(ExpectedVehicleTypeKey)])
    logging::loginfo(paste0("\n\r-- Results --"))
    logging::loginfo(paste0('Matches: ', m,' (', round(m/tot*100, 2),')'))
    logging::loginfo(paste0('Mismatches: ', mm,' (', round(mm/tot*100, 2),')'))
    logging::loginfo(paste0('Unknown vehicle: ', uv,' (', round(uv/tot*100, 2),')'))
    logging::loginfo(paste0('Undefined expected vehicle type: ', ut,' (', round(ut/tot*100, 2),')'))
    
    test_days = seq(floor_date(testDT[,min(dt)], unit='day'), ceiling_date(testDT[,max(dt)], unit='day'), by='1 day')
    steps = length(test_days)-1
    
    # Calculate performance each day since cutoff to present
    for(i in 1:steps) {
      tDT = testDT[dt >= test_days[i] & dt < test_days[i+1]]
      tot = nrow(tDT)
      logging::loginfo(paste0('Day: ', test_days[i], ' --'))
      m = sum(tDT[!is.na(VehicleTypeExternalKey) & !is.na(ExpectedVehicleTypeKey), VehicleTypeExternalKey == ExpectedVehicleTypeKey])
      mm = sum(tDT[!is.na(VehicleTypeExternalKey) & !is.na(ExpectedVehicleTypeKey), VehicleTypeExternalKey != ExpectedVehicleTypeKey])
    
      logging::loginfo(paste0('Matches: ', m,' (', round(m/tot*100, 2),')'))
      logging::loginfo(paste0('Mismatches: ', mm,' (', round(mm/tot*100, 2),')'))
    }
  }
}


