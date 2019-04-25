# Contractor vehicles

library(data.table)
library(lubridate)
library(here)
library(stringr)
library(logging)
library(kableExtra)
library(knitr)
library(openxlsx)


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
  #uDT = uDT[!grepl('^C', eval(Vcol))]
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
  keep2 = shiftsDT[,Service_Type] == 'Direct Delivery'
  return(list('contr'=shiftsDT[keep & keep1], 'direct'=shiftsDT[keep & keep2]))
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
# Plus confirmed invalid vehicles
omit_vehicles = c(tmpDT4[N.d > N.c, CARID], 10095, 993, 114)

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
contrDT = unhiDT2[, .(.N, LastLogon=max(as.Date(CDTS2, tz='UTC')), Units=paste0(unique(UNID), collapse=';'), 
                      LastUnit=.SD[which.max(as.Date(CDTS2, tz='UTC')), UNID]), 
                  by=.(Service,CARID)]
setkey(contrDT, CARID, Service)
contrDT = contrDT[!CARID %in% omit_vehicles]
contrDT = contrDT[!omit_pairs, on=.(CARID, Service)]
contrDT[,New:=ifelse(CARID %in% idsDT[,EHS.NUMBER], 0, 1)]

setorder(contrDT, 'Service')

fwrite(contrDT, here::here(paste0('../data/final/all_contractor_vehicles_',Sys.Date(),'.csv')), sep=",")

# Write xlsx template

vehicle_class <- function(vehid, unhiDT) {
  
  counts = unhiDT[CARID == vehid, .N, by=UNID]
  maxu = counts[which.max(counts[,N]), UNID]
  ut = substr(maxu, 7,7)

  if(ut == 'A' | ut == 'B') {
    uclass = 'AMB'
  } else if(ut == 'T') {
    uclass = 'NAT'
  } else if(ut == 'P' | ut == 'L' | ut == 'S' | ut == 'M' | ut == 'D') {
    uclass = 'SUV'
  } else {
    stop(paste0('Unknown unit type: ', ut))
  }
  
  if(uclass == 'AMB') {
    if(nchar(vehid) != 4) warning(paste('Unusual Vehicle ID for an ambulance:',vehid))
  } else if(uclass == 'NAT') {
    if(nchar(vehid) == 4 & substr(vehid, 1, 1) != '8') warning(paste('Unusual Vehicle ID for an NAT:',vehid))
  } else if(uclass == 'SUV') {
    if(nchar(vehid) == 4 & substr(vehid, 1, 1) != '8' & substr(vehid, 1, 1) != '1') warning(paste('Unusual Vehicle ID for an SUV:',vehid))
  }
  
  return(uclass)
}

excel_form <- function(vDT, service, unhiDT) {
  
  xlsx_add_title <- function(sheet, row_index, title, title_style){
    rows = createRow(sheet,rowIndex=row_index)
    sheet_title = createCell(rows, colIndex=1)
    setCellValue(sheet_title[[1,1]], title)
    setCellStyle(sheet_title[[1,1]], title_style)
  }
  
  wb = createWorkbook(type="xlsx")
  
  # Styles
  title_style1 = CellStyle(wb)+ Font(wb,  heightInPoints=16, color="blue", isBold=TRUE, underline=1)
  title_style2 = CellStyle(wb)+ Font(wb,  heightInPoints=14, isBold=FALSE, underline=0)
  rownames_style = CellStyle(wb) + Font(wb, isBold=TRUE)
  colnames_style = CellStyle(wb) + Font(wb, isBold=TRUE) +
    Alignment(wrapText=TRUE, horizontal="ALIGN_CENTER") +
    Border(color="black", position=c("TOP", "BOTTOM"), 
           pen=c("BORDER_THIN", "BORDER_THICK"))
  
  ex_fill = Fill(foregroundColor="lightgrey")
  ex_font = Font(wb, isItalic = TRUE)
  
  # Divide units into types
  vDT[, UClass:=sapply(CARID, function(v) vehicle_class(v, unhiDT2))]
  
  
  
  # AMBULANCES
  aDT = vDT[UClass == 'AMB']
  a_sheet <- createSheet(wb, sheetName = "Ambulances")
  
  # Title
  xlsx_add_title(a_sheet, row_index=1, title="Ambulances",
                title_style = title_style1)
  
  # Instructions
  xlsx_add_title(a_sheet, row_index=2, 
                title="Please fill in columns D-L for the vehicles in the list. Remove or add vehicles as needed.",
                title_style = title_style2)
  
  # Data
  display.cols = c('EHS Number', 'Last Logon Date', 'Last Logon Unit', 'Stock Level', 'Stretcher Config', 'Stretcher Mount Position', 
                   'Stretcher Load Type', 'Stretcher Type',  'Max Fixed Seats (incl driver)', 'Max Folding Seats', 
                   'Bariatric Level 1 capable', 'Fits Isolette')
  xdt = aDT[order(CARID), `:=`('EHS Number' = CARID, 'Last Logon Date' = LastLogon, 'Last Logon Unit' = LastUnit,
                               'Stock Level' = "", 'Stretcher Config' = "", 'Stretcher Mount Position' = "",  
                               'Stretcher Load Type' = "", 'Stretcher Type' = "",
                               'Max Fixed Seats (incl driver)' = "", 'Max Folding Seats' = "", 
                               'Bariatric Level 1 capable' = "", 'Fits Isolette' = "")][
                                 ,display.cols, with=FALSE]
  examples = list(
    list("Example1", as.Date('2019-04-15'), 'CALG-1A1', 'ALS', 'Single', 'Center', 'Power', 'Stryker Power-PRO XT', '5', '1', 'Yes', 'Yes'),
    list("Example2", as.Date('2019-04-15'), 'CALG-1B2', 'BLS', 'Dual', 'Side', 'Manual', 'Ferno 35X', '3', '1', 'No', 'No'),
    list("Example3", as.Date('2019-04-15'), 'CALG-1A3', 'Bariatric Level 2', 'Single', 'Center', 'Manual', 'Stryker MX-PRO Bariatric', '4', '1', 'Yes', 'No'),
    list("Example4", as.Date('2019-04-15'), 'CALG-1A4', 'NICU', 'Single', 'Center', 'Power', 'Stryker Power-PRO IT', '4', '1', 'No', 'Yes'),
    xdt
  )
  xdt = rbindlist(examples)
  
  
  addDataFrame(xdt, a_sheet, startRow=3, startColumn=1, 
               colnamesStyle = colnames_style, row.names=FALSE)
  setColumnWidth(a_sheet, colIndex=c(1:ncol(xdt)), colWidth=15)
  
  ex_cells = CellBlock(a_sheet, 4, 1, 4, 12, create=FALSE)
  cellrows = rep(1:4, each=12) 
  cellcols = rep(1:12, 4) 
  CB.setFill(ex_cells, ex_fill, cellrows, cellcols)
  CB.setFont(ex_cells, ex_font, cellrows, cellcols)
  
  
  # NATS
  nDT = vDT[UClass == 'NAT']
  n_sheet <- createSheet(wb, sheetName = "NATs")

  # Title
  xlsx_add_title(n_sheet, row_index=1, title="Non-Ambulance Transports (NATs)",
                 title_style = title_style1)

  # Instructions
  xlsx_add_title(n_sheet, row_index=2,
                 title="Please fill in columns D-H for the vehicles in the list. Remove or add vehicles as needed.",
                 title_style = title_style2)

  # Data
  display.cols2 = c('Vehicle Number', 'Last Logon Date', 'Last Logon Unit', 'Max Stretchers', 'Max Wheelchairs', 'Max Ambulatory Patients',
                   'Mixed Use', 'Mixed Use Configurations')
  xdt2 = nDT[order(CARID), `:=`('Vehicle Number' = CARID, 'Last Logon Date' = LastLogon, 'Last Logon Unit' = LastUnit,
                               'Max Stretchers' = "", 'Max Wheelchairs' = "", 'Max Ambulatory Patients' = "",
                               'Mixed Use' = "", 'Mixed Use Configurations' = "")][, display.cols2, with=FALSE]
  examples2 = list(
    list("Example1", as.Date('2019-04-15'), 'CALG-1T1', '2', '0', '0', 'No', ''),
    list("Example2", as.Date('2019-04-15'), 'CALG-1T2', '1', '2', '4', 'Yes', '1 wheelchair + 3 ambulatory / 1 stretcher + 1 ambulatory / 1 stretcher + 1 wheelchair'),
    xdt2
  )
  xdt2 = rbindlist(examples2)

  nc2 = ncol(xdt2)
  nexamples2 = length(examples2)-1
  addDataFrame(xdt2, n_sheet, startRow=3, startColumn=1,
               colnamesStyle = colnames_style, row.names=FALSE)
  setColumnWidth(n_sheet, colIndex=c(1:(nc2-1)), colWidth=18)
  setColumnWidth(n_sheet, colIndex=nc2, colWidth=80)

  ex_cells2 = CellBlock(n_sheet, 4, 1, nexamples2, nc2, create=FALSE)
  cellrows2 = rep(1:nexamples2, each=nc2)
  cellcols2 = rep(1:nc2, nexamples2)
  CB.setFill(ex_cells2, ex_fill, cellrows2, cellcols2)
  CB.setFont(ex_cells2, ex_font, cellrows2, cellcols2)
  
  # # SUVs
  # sDT = vDT[UClass == 'SUV']
  # s_sheet <- createSheet(wb, sheetName = "SUVs")
  # 
  # # Title
  # xlsx_add_title(s_sheet, row_index=1, title="PRU, Supervisor, Manager, Community Paramedic Vehicles",
  #                title_style = title_style1)
  # 
  # # Instructions
  # xlsx_add_title(s_sheet, row_index=2, 
  #                title=paste0("Please verify the list of all SUV vehicles in your fleet ",
  #                             "Add or remove vehicles as needed."),
  #                title_style = title_style2)
  # 
  # # Data
  # display.cols3 = c('Vehicle Number', 'Last Logon Date', 'Last Logon Unit', 'Type')
  # xdt3 = sDT[order(CARID), `:=`('Vehicle Number' = CARID, 'Last Logon Date' = LastLogon, 'Last Logon Unit' = LastUnit,
  #                               'Type'="")][, display.cols3, with=FALSE]
  # examples3 = list(
  #   list("Example1", as.Date('2019-04-15'), 'CALG-1M1', 'Manager'),
  #   list("Example2", as.Date('2019-04-15'), 'CALG-1P1', 'PRU'),
  #   xdt3
  # )
  # xdt3 = rbindlist(examples3)
  # 
  # nc3 = ncol(xdt3)
  # nexamples3 = length(examples3)-1
  # addDataFrame(xdt3, s_sheet, startRow=3, startColumn=1, 
  #              colnamesStyle = colnames_style, row.names=FALSE)
  # setColumnWidth(s_sheet, colIndex=c(1:nc3), colWidth=20)
  # 
  # ex_cells3 = CellBlock(s_sheet, 4, 1, nexamples3, nc3, create=FALSE)
  # cellrows3 = rep(1:nexamples3, each=nc3) 
  # cellcols3 = rep(1:nc3, nexamples3) 
  # CB.setFill(ex_cells3, ex_fill, cellrows3, cellcols3)
  # CB.setFont(ex_cells3, ex_font, cellrows3, cellcols3)
  
  
  options(xlsx.date.format="yyyy-mm-dd")
  #saveWorkbook(wb, here::here(paste0('../data/final/vehicle_survey_',service,'.xlsx')))
}

excel_form2 <- function(vDT, service, unhiDT) {
  
  hs1 <- createStyle(textDecoration = "Bold", 
                     border = c("top", "bottom", 'left', 'right'), borderStyle=c("thin", "double", "thin", "thin"), 
                     valign = 'center', halign = 'center', wrapText = TRUE)
  hs2 <- createStyle(textDecoration = "Bold", 
                     border = c('left', 'right'), borderStyle=c("thin", "thin"), 
                     valign = 'center', halign = 'center', wrapText = TRUE)
  xs1 <- createStyle(textDecoration = "italic", fgFill="lightgrey", wrapText = TRUE, 
                     # border = c('top', 'bottom'), borderStyle=c("thin", "thin")
                     )
  
  
  vDT[, UClass:=sapply(CARID, function(v) vehicle_class(v, unhiDT2))]
  print(table(vDT[,UClass]))
  
  options("openxlsx.dateFormat" = "yyyy-mm-dd")
  wb = createWorkbook()
  addWorksheet(wb, "Ambulances")
  addWorksheet(wb, "NATs")
  addWorksheet(wb, "Config")
  #addWorksheet(wb, "NATs2")
  
  # Drop down config
  dd = data.frame(
    "StretcherConfig" = c("Single", "Dual", "Other"),
    "StretcherMount" = c("Centre", "Side", "Other"),
    "StretcherLoad" = c("Manual", "Power", "Other")
    )
  sc = 1
  sr = 1
  writeData(wb, sheet = 3, dd, startCol = sc, startRow = sr)
  writeData(wb, sheet = 3, c("Binary","Yes","No"), startCol = ncol(dd)+1, startRow = sr)
  writeData(wb, sheet = 3, c("Stock", "ALS", "BLS", "Bariatric Level 2", "NICU", "PICU", "Other"), startCol = ncol(dd)+2, startRow = sr)
  
  # AMBULANCES
  data.cols = c('CARID', 'LastLogon', 'LastUnit')
  aDT = vDT[UClass == 'AMB'][,data.cols, with=FALSE]
  
  xdt = aDT[order(CARID), `:=`('EHS Number' = CARID, 'Last Logon Date' = as.character(LastLogon), 'Last Logon Unit' = LastUnit,
                               'Stock Level' = "", 'Stretcher Config' = "", 'Stretcher Mount Position' = "",  
                               'Stretcher Load Type' = "", 'Stretcher Type' = "",
                               'Max Fixed Seats (incl driver)' = "", 'Max Folding Seats' = "", 
                               'Bariatric Level 1 capable' = "", 'Fits Isolette' = "", 'Comments' = "")]
  xdt = xdt[,!data.cols, with=FALSE]
  examples = list(
    list("Example1", '2019-04-15', 'CALG-1A1', 'ALS', 'Single', 'Center', 'Power', 'Stryker Power-PRO XT', '5', '1', 'Yes', 'Yes', ""),
    list("Example2", '2019-04-15', 'CALG-1B2', 'BLS', 'Dual', 'Side', 'Manual', 'Ferno 35X', '3', '1', 'No', 'No', ""),
    list("Example3", '2019-04-15', 'CALG-1A3', 'Bariatric Level 2', 'Single', 'Center', 'Manual', 'Stryker MX-PRO Bariatric', '4', '1', 'Yes', 'No', ""),
    list("Example4", '2019-04-15', 'CALG-1A4', 'NICU', 'Single', 'Center', 'Power', 'Stryker Power-PRO IT', '4', '1', 'No', 'Yes', ""),
    xdt
  )
  xdt = rbindlist(examples)
  
  sc = 1
  sr = 1
  nex = 4
  writeData(wb, sheet = 1, xdt, startCol = sc, startRow = sr, headerStyle = hs1)
  addStyle(wb, sheet = 1, style = xs1, rows = (sr+1):(nex+sr), cols = 1:ncol(xdt), gridExpand = TRUE)
  setColWidths(wb, sheet = 1, cols = 1:(ncol(xdt)-1), widths=15)
  setColWidths(wb, sheet = 1, cols = ncol(xdt), widths=40)
  
  fillrows = (sr+nex+1):(sr+nrow(xdt))
  dataValidation(wb, sheet = 1, cols = sc+3, rows = fillrows, type = "list", value = "'Config'!$E$2:$E$7") # Stock
  dataValidation(wb, sheet = 1, cols = sc+4, rows = fillrows, type = "list", value = "'Config'!$A$2:$A$4") # Stretcher config
  dataValidation(wb, sheet = 1, cols = sc+5, rows = fillrows, type = "list", value = "'Config'!$B$2:$B$4") # Stretcher position
  dataValidation(wb, sheet = 1, cols = sc+6, rows = fillrows, type = "list", value = "'Config'!$C$2:$C$4") # Strecher load
  dataValidation(wb, sheet = 1, cols = sc+7, rows = fillrows, type = "textLength", operator = "greaterThan", value = 0) # Stretcher model
  dataValidation(wb, sheet = 1, cols = sc+8, rows = fillrows,  type = "whole", operator = "between", value = c(0, 10)) # Fixed seats
  dataValidation(wb, sheet = 1, cols = sc+9, rows = fillrows,  type = "whole", operator = "between", value = c(0, 10)) # Fold seats
  dataValidation(wb, sheet = 1, cols = sc+10, rows = fillrows, type = "list", value = "'Config'!$D$2:$D$3") # Bariatric
  dataValidation(wb, sheet = 1, cols = sc+11, rows = fillrows, type = "list", value = "'Config'!$D$2:$D$3") # Isolette
  
  # NATS
  nDT = vDT[UClass == 'NAT'][,data.cols, with=FALSE]
  
  xdt2 = nDT[order(CARID), `:=`('Vehicle Number' = CARID, 'Last Logon Date' = as.character(LastLogon), 'Last Logon Unit' = LastUnit,
                                'Max Stretchers' = "", 'Max Wheelchairs' = "", 'Max Ambulatory Patients' = "",
                                'Mixed Use' = "", 'Mixed Use Configurations' = "", "Comments" = "")]
  xdt2 = xdt2[,!data.cols, with=FALSE]
  examples2 = list(
    list("Example1", '2019-04-15', 'CALG-1T1', '2', '0', '0', 'No', '', ''),
    list("Example2", '2019-04-15', 'CALG-1T2', '1', '2', '4', 'Yes', '1 wheelchair + 3 ambulatory / 1 stretcher + 1 ambulatory / 1 stretcher + 1 wheelchair', ''),
    xdt2
  )
  xdt2 = rbindlist(examples2)
  
  sc = 1
  sr = 1
  sh = 2
  nex = 2
  writeData(wb, sheet = sh, xdt2, startCol = sc, startRow = sr, headerStyle = hs1)
  addStyle(wb, sheet = sh, style = xs1, rows = (sr+1):(nex+sr), cols = 1:ncol(xdt2), gridExpand = TRUE)
  setColWidths(wb, sheet = sh, cols = 1:(ncol(xdt2)-2), widths=15)
  setColWidths(wb, sheet = sh, cols = (ncol(xdt2)-1):ncol(xdt2), widths=50)
  
  fillrows = (sr+nex+1):(sr+nrow(xdt2))
  dataValidation(wb, sheet = sh, cols = sc+3, rows = fillrows,  type = "whole", operator = "between", value = c(0, 10)) # Stretchers
  dataValidation(wb, sheet = sh, cols = sc+4, rows = fillrows,  type = "whole", operator = "between", value = c(0, 10)) # Wheelchairs
  dataValidation(wb, sheet = sh, cols = sc+5, rows = fillrows,  type = "whole", operator = "between", value = c(0, 20)) # Ambulatory
  dataValidation(wb, sheet = sh, cols = sc+6, rows = fillrows, type = "list", value = "'Config'!$D$2:$D$3") # Mixed
  
  # # NATS v2
  # h1 = list('Vehicle Number', 'Last Logon Date', 'Last Logon Unit',
  #          'Max Stretchers', 'Max Wheelchairs', 'Max Ambulatory Patients',
  #          'Mixed Use', 
  #          'Combined Stretchers + Wheelchairs', '',
  #          'Combined Stretchers + Ambulatory', '',
  #          'Combined Wheelchairs + Ambulatory', '',
  #          'Combined Stretchers + Wheelchairs + Ambulatory', '', '', 'Comments')
  # h2 = list("","","","","","","", 
  #          "Max Stretchers in mixed use config", "Max Wheelchairs in mixed use config", 
  #          "Max Stretchers in mixed use config", "Max Ambulatory in mixed use config",
  #          "Max Wheelchairs in mixed use config", "Max Ambulatory in mixed use config",
  #          "Max Stretchers in mixed use config", "Max Wheelchairs in mixed use config", "Max Ambulatory in mixed use config")
  # 
  # sc = 1
  # sr = 1
  # sh = 4
  # nex = 2
  # nc = length(h1)
  # writeData(wb, sheet = sh, h1, startCol = sc, startRow = sr)
  # writeData(wb, sheet = sh, h2, startCol = sc, startRow = sr+1)
  # for(i in 0:6) {
  #   mergeCells(wb, sheet = sh, cols = sc+i, rows = sr:(sr+1))
  # }
  # for(i in seq(7,12,by=2)) {
  #   mergeCells(wb, sheet = sh, cols = (sc+i):(sc+i+1), rows = sr)
  # }
  # mergeCells(wb, sheet = sh, cols = (sc+13):(sc+15), rows = sr)
  # mergeCells(wb, sheet = sh, cols = (sc+16), rows = sr:(sr+1))
  # 
  # addStyle(wb, sheet = sh, style = hs2, rows = sr, cols = 1:nc, gridExpand = TRUE)
  # addStyle(wb, sheet = sh, style = hs1, rows = sr+1, cols = 1:nc, gridExpand = TRUE)
  # 
  # setColWidths(wb, sheet = sh, cols = 1:7, widths = 15)
  # setColWidths(wb, sheet = sh, cols = 8:nc, widths = 20)
  # 
  # examples31 = list("Example1", '2019-04-15', 'CALG-1T1', '2', '0', '0', 'No', '0', '0', '0', '0', '0', '0', '0', '0', '0')
  # examples32 = list("Example2", '2019-04-15', 'CALG-1T2', '1', '2', '4', 'Yes', '1', '2', '1', '4', '2', '4', '0', '0', '0')
  # 
  # writeData(wb, sheet = sh, examples31, startCol = sc, startRow = sr+2, colNames = FALSE, rowNames = FALSE)
  # writeData(wb, sheet = sh, examples32, startCol = sc, startRow = sr+3, colNames = FALSE, rowNames = FALSE)
  # addStyle(wb, sheet = sh, style = xs1, rows = (sr+2):(nex+sr+1), cols = 1:nc, gridExpand = TRUE)
  # 
  # xdt3 = nDT[order(CARID), `:=`('Vehicle Number' = CARID, 'Last Logon Date' = as.character(LastLogon), 'Last Logon Unit' = LastUnit)]
  # xdt3 = xdt3[,!data.cols, with=FALSE]
  # 
  # writeData(wb, sheet = sh, xdt3, startCol = sc, startRow = sr+nex+2, colNames = FALSE, rowNames = FALSE)
  # 
  # fillrows = (sr+nex+2):(sr+nrow(xdt3))
  # dataValidation(wb, sheet = sh, cols = sc+3, rows = fillrows,  type = "whole", operator = "between", value = c(0, 10)) # Stretchers
  # dataValidation(wb, sheet = sh, cols = sc+4, rows = fillrows,  type = "whole", operator = "between", value = c(0, 10)) # Wheelchairs
  # dataValidation(wb, sheet = sh, cols = sc+5, rows = fillrows,  type = "whole", operator = "between", value = c(0, 10)) # Ambulatory
  # dataValidation(wb, sheet = sh, cols = sc+6, rows = fillrows, type = "list", value = "'Config'!$D$2:$D$3") # Mixed
  # for(i in 7:15) {
  #   dataValidation(wb, sheet = sh, cols = sc+i, rows = fillrows,  type = "whole", operator = "between", value = c(0, 10))
  # }
  # 
  
  saveWorkbook(wb, here::here(paste0('../data/final/vehicle_survey_',service,'.xlsx')), overwrite = TRUE)
}

for(serv in unique(contrDT[,Service])) {
  vDT = contrDT[Service == serv]
  

  serv_label = make.names(serv)
  print(serv_label)
  excel_form2(vDT, serv_label, unhiDT2)
  print('END')
  
}

