# Generate expected unit type
library(stringr)
library(logging)
library(here)
library(data.table)
library(readxl)

logReset()
basicConfig(level='DEBUG')
lfn = here::here('../data/interim/eut.log')
if (file.exists(lfn)) 
  file.remove(lfn)
addHandler(writeToFile, file=lfn, level='DEBUG')

Get_VehHist <- function() {
  # Get most recent 90 shifts of data for each unit looking back at most 1 year
  z_src = 'ARCH_ICAD'
  min_sft_sec = 3 * 3600
  max_date = '20181001'
  max_rec = 90
  z_sql = paste0("WITH [Z1] AS ( ",
                 "SELECT H.[UNID], H.[CARID], H.[CDTS2], [UNIT_STATUS], [TIME_ACTIVE], ",
                 "ROW_NUMBER() OVER (PARTITION BY H.[UNID] ORDER BY H.[CDTS2] DESC) AS [ROW__] ",
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

# CSD_con <- function() {
#   # data source: connect to MS Access Server: 
#   # "\\chfs02.healthy.bewell.ca\PROJECTS\EMS Dispatch CAD\Teams\20 Working Groups\Static Data (Bulk Load) Files\CAD_STATIC_DATA.mdb"
#   #
#   # Method
#   #  'odbc::dbConnect'
#   #
#   # Args
#   #  src: data source, one of {UTILS$Z_CNST$datsrc$SRC}
#   #
#   # Return
#   #  connection object (success) or 1 (fail)
#   
#   # connect
#   z_con <- tryCatch(
#     expr = odbc::dbConnect(
#       drv = odbc::odbc(),
#       .connection_string = "Driver={Microsoft Access Driver (*.mdb, *.accdb)}; Dbq=\\\\chfs02.healthy.bewell.ca\\PROJECTS\\EMS Dispatch CAD\\Teams\\20 Working Groups\\Static Data (Bulk Load) Files\\CAD_STATIC_DATA.mdb;"
#       ),
#     error = function(z_err) return(1L)
#   )
#   if (identical(z_con, 1L)) warning('FAILED ~ CSD_con')
#   
#   # DONE
#   return(z_con)
# }
  
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

Calc_ExpVT <- function(testset_date_cutoff=NA) {
  
  # Retrieve data
  sDT = Get_Shifts()
  sDT = Filter_Shifts(sDT)
  vDT = Get_VehTyp()
  hDT = Get_VehHist()
  
  # Join shift and historical data
  eDT = merge(hDT, sDT, by.x='UNID', by.y='Unit_Name', all.x=FALSE, all.y=TRUE)
  eDT = merge(eDT, vDT, by.x='CARID', by.y='Name', all.x=TRUE, all.y=FALSE)
  
  if(!is.na(testset_date_cutoff)) {
    eDT[,dt:=as.POSIXct(CDTS2, format="%Y-%m-%d %H:%M:%S", tz="UTC")]
    ts_date = as.POSIXct(testset_date_cutoff, format="%Y-%m-%d", tz="UTC")
    testDT = eDT[dt > ts_date]
    eDT = eDT[dt <= ts_date]
    
    # Add back units that do not exist before test date as a "new" unit
    eDT = merge(eDT, sDT, by=names(sDT)[-2], all=TRUE)
    
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
      (cad_ut %in% c('DELTA', 'COMP', 'SUPER', 'ECHO', 'MIKE', 'PRU', 'FRU', 'BPRU', 'ITTEST')), # SUV
      (cad_ut %in% c('DISP', 'SEGWAY', 'ATV', 'FOOT', 'BIKE', 'CART', 'TRAIN', 'BUS', 'MASS', 'HOSP')), # MDT/Non-Transport 
      (cad_ut == "FLIGHT"),
      (cad_ut == "HELI"),
      ((cad_ut == "OTHER") | (cad_ut == "Other")),
      TRUE), 1, which.max)]
    ]
  
  # Assign a resource specific type based on history
  eDT[, grouped_unit:=ifelse(str_detect(UNID,"^[A-Z]{4}\\-\\d[AB]\\d+$"), paste0(str_sub(UNID,1,6),'_',str_sub(UNID,8,100)), UNID)]
  
  # When multiple candidate types exist, select type with lowest capabilities
  type_ranking <- function(typ) {
    typ = typ[!is.na(typ)]
    
    if(length(typ) == 0) return(NA)
    
    df = data.table(typ=typ, c=0, s=0, w=0, a=0, d=0)
    df[, typ:=str_replace(typ, '^UT_Air_', '')]
    df[, typ:=str_replace(typ, '^UT_', '')]
    
    # Increment types scores
    df[str_sub(typ,6,6)=="S", c:=1] # SUV
    df[str_sub(typ,6,6)=="N", c:=2] # NAT
    df[str_sub(typ,6,6)=="", c:=3] # AMB
    df[str_sub(typ,6,6)=="H", c:=4] # HELI
    df[str_sub(typ,6,6)=="H", c:=5] # FLIGHT
    
    # Increment stretcher types scores
    df[str_sub(typ,1,4)=="SS00", s:=1]
    df[str_sub(typ,1,4)=="SC00", s:=2]
    df[str_sub(typ,1,4)=="DS00", s:=3]
    df[str_sub(typ,1,4)=="SW00", s:=3.5]
    df[str_sub(typ,1,4)=="SS01", s:=4]
    df[str_sub(typ,1,4)=="SC01", s:=5]
    df[str_sub(typ,1,4)=="DS01", s:=6]
    df[str_sub(typ,1,4)=="SW01", s:=6.5]
    df[str_sub(typ,1,4)=="SS10", s:=7]
    df[str_sub(typ,1,4)=="SC10", s:=8]
    df[str_sub(typ,1,4)=="DS10", s:=9]
    df[str_sub(typ,1,4)=="SW10", s:=9.5]
    df[str_sub(typ,1,4)=="SS11", s:=10]
    df[str_sub(typ,1,4)=="SC11", s:=11]
    df[str_sub(typ,1,4)=="DS11", s:=12]
    df[str_sub(typ,1,4)=="SW11", s:=12.5]
    
    # Increment wheelchairs
    stopifnot(all(df[str_sub(typ,1,2)=="SW" | str_sub(typ,1,2)=="WA", str_sub(typ,7,100) != ""]))
    df[str_sub(typ,1,2)=="SW" | str_sub(typ,1,2)=="WA", w:=as.numeric(str_sub(typ,7,100)) ]
    
    # Increment all capacity scores
    df[,a:=as.numeric(str_sub(typ,5,5))]
    
    # Increment air speed scores
    stopifnot(all(df[str_sub(typ,6,6)=="H" | str_sub(typ,6,6)=="A", str_sub(typ,7,100) != ""]))
    df[str_sub(typ,6,6)=="H" | str_sub(typ,6,6)=="A", d:=as.numeric(str_sub(typ,7,100)) ]
    
    # Find row with lowest score using relative importance as follows:
    setorder(df, c('c', 's', 'w', 'a', 'd', 'typ'))
    
    return(df[!is.na(typ),typ][1])
  }
  
  # Identify expected 
  expected_unit <- function(dt) {
    lookback=14
    prop=.6
    
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
      logdebug(paste(logmsg, 'no history. Selecting default:',default_veh))
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

    logdebug(logmsg)
    return(as.character(typ))
  }
  
  eDT[, c("ExpectedVehicleTypeKey"):=expected_unit(.SD), by=grouped_unit]
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
    logdebug(paste0('-- Test Results --'))
    m = sum(testDT[!is.na(VehicleTypeExternalKey) & !is.na(ExpectedVehicleTypeKey), VehicleTypeExternalKey == ExpectedVehicleTypeKey])
    mm = sum(testDT[!is.na(VehicleTypeExternalKey) & !is.na(ExpectedVehicleTypeKey), VehicleTypeExternalKey != ExpectedVehicleTypeKey])
    uv = sum(testDT[, is.na(VehicleTypeExternalKey)])
    ut = sum(testDT[, is.na(ExpectedVehicleTypeKey)])
    logdebug(paste0('Matches: ', m,' (', round(m/tot*100, 2),')'))
    logdebug(paste0('Mismatches: ', mm,' (', round(mm/tot*100, 2),')'))
    logdebug(paste0('Unknown vehicle: ', uv,' (', round(uv/tot*100, 2),')'))
    logdebug(paste0('Undefined expected vehicle type: ', ut,' (', round(ut/tot*100, 2),')'))
  } else {
    testDT = NULL
  }
  
  return(list(eDT2, testDT))
}

Cmp_ExpVT <- function(f, kcol='External Key', tcol='Expected Unit Type') {
  
  rs = Calc_ExpVT()
  eDT = rs[[1]]
  rDT = as.data.table(read_xlsx(f, sheet = 1, col_types = 'text'))
  cols = c(kcol, tcol)
  rDT = rDT[,cols, with=FALSE]
  
  lutDT = merge(rDT, eDT, by.x=kcol, by.y="ExternalKey", all.x = TRUE, all.y = FALSE)
  
  logdebug(paste0("The following units are missing expected unit types (and will not be updated):\n", 
                  paste0(lutDT[is.na(ExpectedVehicleTypeKey), `External Key`], collapse=',')))
  
  # Changed unit types
  outcols = c("ExpectedVehicleTypeKey", "External Key")
  lutDT = lutDT[("ExpectedVehicleTypeKey" != tcol), outcols, with=FALSE][!is.na(ExpectedVehicleTypeKey)]
  
  logdebug(paste0("The following units have complex unit types that cannot be automatically updated:\n", 
                  paste0(lutDT[ExpectedVehicleTypeKey == "Complex", `External Key`], collapse=',')))
  
  lutDT = lutDT[ExpectedVehicleTypeKey != "Complex"]
  
  return(lutDT)
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
  logdebug(paste0("Inserting ",nr," new records"))
  for(i in 1:5) {
    z_sql = paste0("INSERT INTO tbl_VehicleType_expected (Grouped_Unit_Name, LUT_Key, Expected_Vehicle_Type_Key) VALUES ('",
                   paste(nDT[i, .(grouped_unit, ExternalKey, ExpectedVehicleTypeKey)], collapse="','"), "');")
    
    z_i = DatSrc_exec(z_con, z_sql)
  
    if(z_i == 1) {
      logdebug(paste0('Inserted record ',nDT[i, ExternalKey]))
    } else {
      logdebug(paste0('Insertion failed for record ',nDT[i, ExternalKey]))
    }
  }
  
  # Updated changed records
  uDT = mDT[!is.na(Expected_Vehicle_Type_Key) & Expected_Vehicle_Type_Key != ExpectedVehicleTypeKey]
  nr = nrow(uDT)
  logdebug(paste0("Updating ",nr," records"))
  for(i in 1:nr) {
    z_sql = paste0("UPDATE tbl_VehicleType_expected SET [Expected_Vehicle_Type_Key] = '", uDT[i,ExpectedVehicleTypeKey],
                   "' WHERE [LUT_Key] = '",uDT[i,ExternalKey],"';")
    
    z_i = DatSrc_exec(z_con, z_sql)
    
    if(z_i == 1) {
      logdebug(paste0('Updated record ',nDT[i, ExternalKey], ' with new VT: ', nDT[i, ExpectedVehicleTypeKey]))
    } else {
      logdebug(paste0('Update failed for record ',nDT[i, ExternalKey]))
    }
    
  }
  
  UTILS$DatSrc_dis(z_con)
}


DatSrc_exec <- function(con, sql) {
  
  runone <- function(con, sql) {
    rs = odbc::dbSendStatement(con, sql)
    rows = odbc::dbGetRowsAffected(rs)
    odbc::dbClearResult(rs)
    
    return(rows)
  }
  
  z_i <- tryCatch(
    runone(con, sql), 
    error = function(z_err) return(-1L)
  )
  if (identical(z_i, -1L)) {
    warning('FAILED ~ DatIns')
  }
  
  return(z_i)
}

#updDT = Cmp_ExpVT(f='../data/raw/TEST_LIDS_Resource_Template_2019-09-27_14-17-06.xlsx', kcol='External Key', tcol='Expected Unit Type')

#fwrite(updDT, file='../data/final/TEST_LIDS_LUT_VehTyp_Update_2019-09-27_14-17-06.csv')


