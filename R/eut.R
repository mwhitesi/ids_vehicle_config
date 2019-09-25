# Generate expected unit type
library(stringr)
library(logging)
library(here)

logReset()
basicConfig(level='DEBUG')
lfn = here::here('../data/interim/eut.log')
if (file.exists(lfn)) 
  file.remove(lfn)
addHandler(writeToFile, file=lfn, level='DEBUG')

Get_VehHist <- function() {
  
  z_src = 'ARCH_ICAD'
  min_sft_sec = 1.5 * 3600
  max_date = '20171231'
  z_sql = paste0("SELECT H.[UNID], H.[CARID], H.[CDTS2], [UNIT_STATUS], [TIME_ACTIVE] ",
  "FROM [AHS_ARCH].[dbo].[UN_HI] H, [AHS_ARCH].[dbo].[UNIT_WKLOAD] W ",
  "WHERE CARID IS NOT NULL AND ",
  "LEFT(H.CDTS,8) > '",max_date,"' AND ", 
  "UNIT_STATUS = 'LO' AND ",
  "H.CDTS = W.CDTS AND ",
  "H.UNID = W.UNID AND ",
  "TIME_ACTIVE >= ",min_sft_sec)
  
  z_con <- UTILS$DatSrc_con(z_src)
  z_dat <- UTILS$DatSrc_get(z_con, z_sql)
  z_dis <- UTILS$DatSrc_dis(z_con)
  
  return(z_dat)
}

Get_VehTyp <- function() {
  
  z_src = 'PRDa_LIDS'
  z_sql = paste0("SELECT [Name], [ExternalKey], [VehicleTypeExternalKey] ",
    "FROM [LogisCAD_DwhStd].[dbo].[DwhVehicle] ",
    "WHERE [DeletedStatus] = 0")
  
  z_con <- UTILS$DatSrc_con(z_src)
  z_dat <- UTILS$DatSrc_get(z_con, z_sql)
  z_dis <- UTILS$DatSrc_dis(z_con)
  
  return(z_dat)
}

CSD_con <- function() {
  # data source: connect to MS Access Server: 
  # "\\chfs02.healthy.bewell.ca\PROJECTS\EMS Dispatch CAD\Teams\20 Working Groups\Static Data (Bulk Load) Files\CAD_STATIC_DATA.mdb"
  #
  # Method
  #  'odbc::dbConnect'
  #
  # Args
  #  src: data source, one of {UTILS$Z_CNST$datsrc$SRC}
  #
  # Return
  #  connection object (success) or 1 (fail)
  
  # connect
  z_con <- tryCatch(
    expr = odbc::dbConnect(
      drv = odbc::odbc(),
      .connection_string = "Driver={Microsoft Access Driver (*.mdb, *.accdb)}; Dbq=\\\\chfs02.healthy.bewell.ca\\PROJECTS\\EMS Dispatch CAD\\Teams\\20 Working Groups\\Static Data (Bulk Load) Files\\CAD_STATIC_DATA.mdb;"
      ),
    error = function(z_err) return(1L)
  )
  if (identical(z_con, 1L)) warning('FAILED ~ CSD_con')
  
  # DONE
  return(z_con)
}
  
Get_Shifts <- function() {
  
  # Connect to CSD
  z_con = CSD_con()
  
  # Retrieve
  z_sql = paste0("SELECT * ",
    "FROM tbl_ShiftInventory AS SI ",
    "WHERE SI.[Active/Inactive]='Active' ",
    "ORDER BY SI.Unit_Name;")
  
  df1 = UTILS$DatSrc_get(z_con, z_sql)
  
  UTILS$DatSrc_dis(z_con)
  
  return(df1)
}

Filter_Shifts <- function(df1) {
  ut = c("ALS","BLS","PRU","ENAT","BNAT","EMR","HELI","WING","FLIGHT","ALSr","BPRU", "COMP", "SUPER", "DELTA", "ECHO", "MIKE")
  df1[ (Unit_Name %in% c("PICT-5A1","PICT-5B1") & Days == "Weekend") | (!(Unit_Name %in% c("PICT-5A1","PICT-5B1")) & `CAD Unit Type` %in% ut)]
  
  return(df1)
}

Calc_ExpVT <- function() {
  
  # Retrieve data
  sDT = Get_Shifts()
  sDT = Filter_Shifts(sDT)
  vDT = Get_VehTyp()
  hDT = Get_VehHist()
  
  # Join shift and historical data
  eDT = merge(hDT, sDT, by.x='UNID', by.y='Unit_Name', all=FALSE)
  eDT = merge(eDT, vDT, by.x='CARID', by.y='Name', all.x=TRUE, all.y=FALSE)
  
  # Assign a default type to use for new units or old/obsolute units that we have no login history
  eDT[,veh:=ifelse(suppressWarnings(is.na(as.integer(CARID))), -1, suppressWarnings(as.integer(CARID)))]
  eDT[,cad_ut:=str_sub(UNID, 7, 7)]
  
  eDT[, default_ut:= c("Complex", "UT_AA002N", "UT_SC002", "UT_AA002S", "UT_Standalone_MDT", "Undefined")[
    apply(cbind(
      (veh < 200),
      ((veh > 4999 & veh < 9000) | (veh > 19999 & veh < 40000)), # NAT
      (veh > 999 & veh < 5000), # Ambulance
      ((veh > 39999 & veh < 50000) | (veh > 9999 & veh < 19999)), # SUV
      ((veh > 899 & veh < 1000) | (veh > 8999 & veh < 10000)), # MDT
      TRUE), 1, which.max)]
    ]
  
  # Assign a resource specific type based on history
  eDT[, grouped_unit:=ifelse(str_sub(UNID,7,7)=="A" | str_sub(UNID,7,7)=="B", paste0(str_sub(UNID,1,6),'_',str_sub(UNID,8,100)), UNID)]
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
    # stopifnot(all(df[str_sub(typ,1,2)=="SW" | str_sub(typ,1,2)=="WA", str_sub(typ,7,100) != ""]))
    # df[str_sub(typ,1,2)=="SW" | str_sub(typ,1,2)=="WA", w:=as.numeric(str_sub(typ,7,100)) ]
    
    # Increment all capacity scores
    df[,a:=as.numeric(str_sub(typ,5,5))]
    
    # Increment air speed scores
    # stopifnot(all(df[str_sub(typ,6,6)=="H" | str_sub(typ,6,6)=="A", str_sub(typ,7,100) != ""]))
    # df[str_sub(typ,6,6)=="H" | str_sub(typ,6,6)=="A", d:=as.numeric(str_sub(typ,7,100)) ]
    
    # Find row with lowest score using relative importance as follows:
    setorder(df, c('c', 's', 'w', 'a', 'd', 'typ'))
    
    return(df[is.na(typ),typ])
  }
  
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
        logmsg = suppressWarnings(paste(logmsg, 'inconsistent type, selecting minimum:',minvt,'from',vt))
        
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
  eDT2 = unique(eDT, by=c("ExpectedVehicleTypeKey", "ExternalKey"))[,.(ExternalKey, ExpectedVehicleTypeKey)]
  
  eDT2
}
  
  
eDT2[ExternalKey=="LUT_BLAI-4B2"]
eDT2[ExternalKey=="LUT_BLAI-4A2"]

