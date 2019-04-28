# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
@create  : 20190206 (yyyyMMdd)
@author  : Omegal Gangapersad
@Purpose : Read raw data for BBBEE analysis which consists of:
				- BBBEE, this consists of BBBEE related data such as aggregate BBBEE score retrieved from Intellidex, Empowerdex and the research of Van der Merwe and Ferreira
				- PRICE, this is the price of companies in SEC file retrieved from Thomson Reuters Datastream
				- BVPS, this is the book value per share (value factor FamaFrench) of companies in SEC file retrieved from Thomson Reuters Datastream
				- MV, this is the market value (size factor FamaFrench) of companies in SEC file retrieved from Thomson Reuters Datastream
				- YLD, this consists of various yield (to calculate market premium factor FamaFrench) retrieved from Thomson Reuters Datastream
				- SEC, this consists of the securities found in the BBBEE file, mapped to Thomson Reuters Datastream identifiers
				@instruct:  Do not remove line 1 and 2. Code in this script on line 2 is used to be able to import other scripts as modules.  
            Under the declare sections adjust: 
				- the importdirectory to the user's selected directory - always
				- the BBBEE, PRICE, BVPS, MV, YLD or SEC - if user altered source filenames
@update  : 	Date (yyyyMMdd)
			  20190215:
                - Added logs
                - Cleaned structure of this script
            20190424:
                - Restructured this script
                - Created Settings file to base this script on
                - Inserted download from into the script
            20190428:
                - Added Sector classification
                - Cleaned up the TRY to make sure it only downloads when files are not available
                - Replaced SECID with FirmID                          
"""
##START SCRIPT
import os
import pandas as pd
import Functions
import datetime

def RawDataExcelToPython(ScriptName,ImportDirectory,FilenameBBBEE,FilenamePRICE,FilenameBVPS,FilenameMV,FilenameYLD,FilenameFIRM,SheetGeneral,SheetFIRMDS,SheetFIRMBBBEEDS,FilenameSECTOR):    
    global RawDataBBBEE, RawDataPRICE, RawDataBVPS, RawDataMV, RawDataYLD, RawDataFIRMDS, RawDataFIRMBBBEEDS, RawDataSECTOR
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataBBBEE')
    RawDataBBBEE = pd.read_excel(ImportDirectory + FilenameBBBEE['Value'].iloc[0],SheetGeneral)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataPRICE')
    RawDataPRICE = pd.read_excel(ImportDirectory + FilenamePRICE['Value'].iloc[0],SheetGeneral)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataBVPS')
    RawDataBVPS = pd.read_excel(ImportDirectory + FilenameBVPS['Value'].iloc[0],SheetGeneral)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataMV')
    RawDataMV = pd.read_excel(ImportDirectory + FilenameMV['Value'].iloc[0],SheetGeneral)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataYLD')
    RawDataYLD = pd.read_excel(ImportDirectory + FilenameYLD['Value'].iloc[0],SheetGeneral)   
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataFIRMDS')
    RawDataFIRMDS = pd.read_excel(ImportDirectory + FilenameFIRM['Value'].iloc[0],SheetFIRMDS)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataFIRMBBBEEDS')
    RawDataFIRMBBBEEDS = pd.read_excel(ImportDirectory + FilenameFIRM['Value'].iloc[0],SheetFIRMBBBEEDS)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataSECTOR')
    RawDataSECTOR = pd.read_excel(ImportDirectory + FilenameSECTOR['Value'].iloc[0],SheetGeneral)
    return RawDataBBBEE, RawDataPRICE, RawDataBVPS, RawDataMV, RawDataYLD, RawDataFIRMDS, RawDataFIRMBBBEEDS, RawDataSECTOR

def DownloadRawData(ScriptName,ImportDirectory,Settings,FilenameBBBEE,FilenamePRICE,FilenameBVPS,FilenameMV,FilenameYLD,FilenameFIRM, FilenameSECTOR):
    IDBBBEE = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameBBBEE']['Comment'])
    IDPRICE = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenamePRICE']['Comment'])
    IDBVPS = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameBVPS']['Comment'])
    IDMV = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameMV']['Comment'])
    IDYLD = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameYLD']['Comment'])
    IDFIRM = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameFIRM']['Comment'])
    IDSECTOR = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameSECTOR']['Comment'])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataBBBEE')
    Functions.download_file_from_google_drive(IDBBBEE['Comment'].iloc[0], ImportDirectory + FilenameBBBEE['Value'].iloc[0])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataPRICE')
    Functions.download_file_from_google_drive(IDPRICE['Comment'].iloc[0], ImportDirectory + FilenamePRICE['Value'].iloc[0])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataBVPS')
    Functions.download_file_from_google_drive(IDBVPS['Comment'].iloc[0], ImportDirectory + FilenameBVPS['Value'].iloc[0])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataMV')
    Functions.download_file_from_google_drive(IDMV['Comment'].iloc[0], ImportDirectory + FilenameMV['Value'].iloc[0])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataYLD')
    Functions.download_file_from_google_drive(IDYLD['Comment'].iloc[0], ImportDirectory + FilenameYLD['Value'].iloc[0])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataFIRM')
    Functions.download_file_from_google_drive(IDFIRM['Comment'].iloc[0], ImportDirectory + FilenameFIRM['Value'].iloc[0])    
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataSECTOR')
    Functions.download_file_from_google_drive(IDSECTOR['Comment'].iloc[0], ImportDirectory + FilenameSECTOR['Value'].iloc[0])    

    
##START SCRIPT
tmpScriptName = os.path.basename(__file__)
tmpStartTimeScript = datetime.datetime.now()
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start Script') 
tmpMainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
tmpImportDirectory = tmpMainDir + '\\Input\\RawData\\' 
tmpSettings = pd.read_excel(tmpMainDir + '\\Settings.xlsx','Sheet1')
tmpFilenameBBBEE = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameBBBEE']['Value'])
tmpFilenamePRICE = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenamePRICE']['Value'])
tmpFilenameBVPS = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameBVPS']['Value'])
tmpFilenameMV = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameMV']['Value'])
tmpFilenameYLD = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameYLD']['Value'])
tmpFilenameFIRM = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameFIRM']['Value'])
tmpFilenameSECTOR = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameSECTOR']['Value'])

tmpSheetGeneral = 'Output'
tmpSheetFIRMDS = 'Unique_RIC_ISIN_Mapping' #RIC retrieved data
tmpSheetFIRMBBBEEDS = 'SourceName_RIC_Mapping' #BBBEE name RIC mapping

##READ RAWDATA
try:    
    RawDataExcelToPython(tmpScriptName,tmpImportDirectory,tmpFilenameBBBEE,tmpFilenamePRICE,tmpFilenameBVPS,tmpFilenameMV,tmpFilenameYLD,tmpFilenameFIRM,tmpSheetGeneral,tmpSheetFIRMDS,tmpSheetFIRMBBBEEDS,tmpFilenameSECTOR)
except:
    DownloadRawData(tmpScriptName,tmpImportDirectory,tmpSettings,tmpFilenameBBBEE,tmpFilenamePRICE,tmpFilenameBVPS,tmpFilenameMV,tmpFilenameYLD,tmpFilenameFIRM,tmpFilenameSECTOR)
    RawDataExcelToPython(tmpScriptName,tmpImportDirectory,tmpFilenameBBBEE,tmpFilenamePRICE,tmpFilenameBVPS,tmpFilenameMV,tmpFilenameYLD,tmpFilenameFIRM,tmpSheetGeneral,tmpSheetFIRMDS,tmpSheetFIRMBBBEEDS,tmpFilenameSECTOR)

##END SCRIPT
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'End Script, RunTime: '+ Functions.StrfTimeDelta(datetime.datetime.now()-tmpStartTimeScript))
del tmpScriptName, tmpStartTimeScript, tmpMainDir, tmpImportDirectory, tmpSettings, tmpFilenameBBBEE, tmpFilenamePRICE, tmpFilenameBVPS, tmpFilenameMV, tmpFilenameYLD, tmpFilenameFIRM, tmpSheetGeneral, tmpSheetFIRMDS, tmpSheetFIRMBBBEEDS