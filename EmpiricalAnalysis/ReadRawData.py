# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
@create  : 20190206 (yyyyMMdd)
@author  : Omegal Gangapersad
@Purpose : Read raw data for BBBEE analysis which consists of:
				- BBBEE, this consists of BBBEE related data such as aggregate BBBEE score retrieved from Intellidex, Empowerdex and the research of Van der Merwe and Ferreira
				- PRICE, this is the price of companies in SEC file retrieved from Thomson Reuters Datastream
				- BVPS, this is the book value per share (value factor FamaFrench) of companies in SEC file retrieved from Thomson Reuters Datastream
				- SIZE, this is the market value (size factor FamaFrench) of companies in SEC file retrieved from Thomson Reuters Datastream
				- YLD, this consists of various yield (to calculate market premium factor FamaFrench) retrieved from Thomson Reuters Datastream
				- SEC, this consists of the securities found in the BBBEE file, mapped to Thomson Reuters Datastream identifiers
@instruct:  Do not remove line 1 and 2. Code in this script on line 2 is used to be able to import other scripts as modules.  
            Under the declare sections adjust: 
				- the importdirectory to the user's selected directory - always
				- the BBBEE, PRICE, BVPS, SIZE, YLD or SEC - if user altered source filenames
@update  :  Date (yyyyMMdd)
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
                - Replaced MV with SIZE 
            20190505:
                - Added JSE (constituents)
            20190508:
                - Added Earnings to Price (E2P)     
            20190526:
                - Cleaned code, made it robust to create rawdata folder                                           
"""
##START SCRIPT
import os
import pandas as pd
import Functions
import datetime

def RawDataExcelToPython(ScriptName,ImportDirectory,FilenameBBBEE,FilenamePRICE,FilenameBVPS,FilenameSIZE,FilenameYLD,FilenameFIRM,SheetGeneral,SheetFIRMDS,SheetFIRMBBBEEDS,FilenameSECTOR,FilenameJSE,FilenameE2P):    
    global RawDataBBBEE, RawDataPRICE, RawDataBVPS, RawDataSIZE, RawDataYLD, RawDataFIRMDS, RawDataFIRMBBBEEDS, RawDataSECTOR, RawDataJSE, RawDataE2P
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataBBBEE')
    RawDataBBBEE = pd.read_excel(ImportDirectory + FilenameBBBEE['Value'].iloc[0],SheetGeneral)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataPRICE')
    RawDataPRICE = pd.read_excel(ImportDirectory + FilenamePRICE['Value'].iloc[0],SheetGeneral)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataBVPS')
    RawDataBVPS = pd.read_excel(ImportDirectory + FilenameBVPS['Value'].iloc[0],SheetGeneral)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataSIZE')
    RawDataSIZE = pd.read_excel(ImportDirectory + FilenameSIZE['Value'].iloc[0],SheetGeneral)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataYLD')
    RawDataYLD = pd.read_excel(ImportDirectory + FilenameYLD['Value'].iloc[0],SheetGeneral)   
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataFIRMDS')
    RawDataFIRMDS = pd.read_excel(ImportDirectory + FilenameFIRM['Value'].iloc[0],SheetFIRMDS)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataFIRMBBBEEDS')
    RawDataFIRMBBBEEDS = pd.read_excel(ImportDirectory + FilenameFIRM['Value'].iloc[0],SheetFIRMBBBEEDS)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataSECTOR')
    RawDataSECTOR = pd.read_excel(ImportDirectory + FilenameSECTOR['Value'].iloc[0],SheetGeneral)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataJSE')
    RawDataJSE = pd.read_excel(ImportDirectory + FilenameJSE['Value'].iloc[0],SheetGeneral)
    Functions.LogScript(ScriptName,datetime.datetime.now(),'RawDataExcelToPython: RawDataE2P')
    RawDataE2P = pd.read_excel(ImportDirectory + FilenameE2P['Value'].iloc[0],SheetGeneral)
    return RawDataBBBEE, RawDataPRICE, RawDataBVPS, RawDataSIZE, RawDataYLD, RawDataFIRMDS, RawDataFIRMBBBEEDS, RawDataSECTOR, RawDataJSE, RawDataE2P

def DownloadRawData(ScriptName,ImportDirectory,Settings,FilenameBBBEE,FilenamePRICE,FilenameBVPS,FilenameSIZE,FilenameYLD,FilenameFIRM, FilenameSECTOR,FilenameJSE,FilenameE2P) :    
    IDBBBEE = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameBBBEE']['Comment'])
    IDPRICE = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenamePRICE']['Comment'])
    IDBVPS = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameBVPS']['Comment'])
    IDSIZE = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameSIZE']['Comment'])
    IDYLD = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameYLD']['Comment'])
    IDFIRM = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameFIRM']['Comment'])
    IDSECTOR = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameSECTOR']['Comment'])
    IDJSE = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameJSE']['Comment'])
    IDE2P = pd.DataFrame(Settings[Settings['Parameter'] == 'FilenameE2P']['Comment'])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataBBBEE')
    Functions.download_file_from_google_drive(IDBBBEE['Comment'].iloc[0], ImportDirectory + FilenameBBBEE['Value'].iloc[0])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataPRICE')
    Functions.download_file_from_google_drive(IDPRICE['Comment'].iloc[0], ImportDirectory + FilenamePRICE['Value'].iloc[0])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataBVPS')
    Functions.download_file_from_google_drive(IDBVPS['Comment'].iloc[0], ImportDirectory + FilenameBVPS['Value'].iloc[0])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataSIZE')
    Functions.download_file_from_google_drive(IDSIZE['Comment'].iloc[0], ImportDirectory + FilenameSIZE['Value'].iloc[0])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataYLD')
    Functions.download_file_from_google_drive(IDYLD['Comment'].iloc[0], ImportDirectory + FilenameYLD['Value'].iloc[0])
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataFIRM')
    Functions.download_file_from_google_drive(IDFIRM['Comment'].iloc[0], ImportDirectory + FilenameFIRM['Value'].iloc[0])    
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataSECTOR')
    Functions.download_file_from_google_drive(IDSECTOR['Comment'].iloc[0], ImportDirectory + FilenameSECTOR['Value'].iloc[0])    
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataJSE')
    Functions.download_file_from_google_drive(IDJSE['Comment'].iloc[0], ImportDirectory + FilenameJSE['Value'].iloc[0])    
    Functions.LogScript(ScriptName,datetime.datetime.now(),'Download: RawDataE2P')
    Functions.download_file_from_google_drive(IDE2P['Comment'].iloc[0], ImportDirectory + FilenameE2P['Value'].iloc[0])    

def main():
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
    tmpFilenameSIZE = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameSIZE']['Value'])
    tmpFilenameYLD = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameYLD']['Value'])
    tmpFilenameFIRM = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameFIRM']['Value'])
    tmpFilenameSECTOR = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameSECTOR']['Value'])
    tmpFilenameJSE = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameJSE']['Value'])
    tmpFilenameE2P = pd.DataFrame(tmpSettings[tmpSettings['Parameter'] == 'FilenameE2P']['Value'])
    
    tmpSheetGeneral = 'Output'
    tmpSheetFIRMDS = 'Unique_RIC_ISIN_Mapping' #RIC retrieved data
    tmpSheetFIRMBBBEEDS = 'SourceName_RIC_Mapping' #BBBEE name RIC mapping
    
    ##CHECK FOR EXISTENCE RAWDATA FOLDER   
    try:    
        try:
            os.mkdir(tmpMainDir + '\\Input\\')
            os.mkdir(tmpImportDirectory)
        except:
            os.mkdir(tmpImportDirectory)            
        Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Created Input RawData folder') 
    except:
        Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Input RawData folder exists') 
            
    ##READ RAWDATA
    try:    
        RawDataExcelToPython(tmpScriptName,tmpImportDirectory,tmpFilenameBBBEE,tmpFilenamePRICE,tmpFilenameBVPS,tmpFilenameSIZE,tmpFilenameYLD,tmpFilenameFIRM,tmpSheetGeneral,tmpSheetFIRMDS,tmpSheetFIRMBBBEEDS,tmpFilenameSECTOR, tmpFilenameJSE, tmpFilenameE2P)
    except:
        DownloadRawData(tmpScriptName,tmpImportDirectory,tmpSettings,tmpFilenameBBBEE,tmpFilenamePRICE,tmpFilenameBVPS,tmpFilenameSIZE,tmpFilenameYLD,tmpFilenameFIRM,tmpFilenameSECTOR,tmpFilenameJSE,tmpFilenameE2P)
        RawDataExcelToPython(tmpScriptName,tmpImportDirectory,tmpFilenameBBBEE,tmpFilenamePRICE,tmpFilenameBVPS,tmpFilenameSIZE,tmpFilenameYLD,tmpFilenameFIRM,tmpSheetGeneral,tmpSheetFIRMDS,tmpSheetFIRMBBBEEDS,tmpFilenameSECTOR,tmpFilenameJSE, tmpFilenameE2P)
    
    ##END SCRIPT
    Functions.LogScript(tmpScriptName,datetime.datetime.now(),'End Script, RunTime: '+ Functions.StrfTimeDelta(datetime.datetime.now()-tmpStartTimeScript))
    del tmpScriptName, tmpStartTimeScript, tmpMainDir, tmpImportDirectory, tmpSettings, tmpFilenameBBBEE, tmpFilenamePRICE, tmpFilenameBVPS, tmpFilenameSIZE, tmpFilenameYLD, tmpFilenameFIRM, tmpSheetGeneral, tmpSheetFIRMDS, tmpSheetFIRMBBBEEDS, tmpFilenameSECTOR, tmpFilenameJSE, tmpFilenameE2P

if __name__ == '__main__':
    main()