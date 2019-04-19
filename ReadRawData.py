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
			20190215
				- Added logs
				- Cleaned structure of this script
"""
##START SCRIPT
import os
import pandas as pd
import Functions
import datetime
tmpScriptName = os.path.basename(__file__)
tmpStartTimeScript = datetime.datetime.now()
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start Script')
    
##INITIATE
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Initiate')
MainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
ImportDirectory = MainDir + '\\Input\\RawData\\' #'C:/Users/neera/OneDrive/Documents/MasterThesis/Data/'
FilenameBBBEE = 'BBBEE_aggregate.xlsx'
SheetBBBEE = 'Sheet1'
FilenamePRICE = 'Datastream_Price_Yr.xlsx'
SheetPRICE = 'Output'
FilenameBVPS = 'Datastream_BookvaluePerShare_Yr.xlsx'
SheetBVPS = 'Output'
FilenameMV = 'Datastream_MarketValue_Yr.xlsx'
SheetMV = 'Output'
FilenameYLD = 'Datastream_Yields_Yr.xlsx'
SheetYLD = 'Output'
FilenameSEC ='SecurityIdentifiers.xlsx'
SheetSECDS = 'Unique_RIC_ISIN_Mapping' #RIC retrieved data
SheetSECBBBEEDS = 'SourceName_RIC_Mapping' #BBBEE name RIC mapping

##CALCULATE
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Calculate')
RawDataBBBEE = pd.read_excel(ImportDirectory + FilenameBBBEE,SheetBBBEE)
RawDataPRICE = pd.read_excel(ImportDirectory + FilenamePRICE,SheetPRICE)
RawDataBVPS = pd.read_excel(ImportDirectory + FilenameBVPS,SheetBVPS)
RawDataMV = pd.read_excel(ImportDirectory + FilenameMV,SheetMV)
RawDataYLD = pd.read_excel(ImportDirectory + FilenameYLD,SheetYLD)
RawDataSECDS = pd.read_excel(ImportDirectory + FilenameSEC,SheetSECDS)
RawDataSECBBBEEDS = pd.read_excel(ImportDirectory + FilenameSEC,SheetSECBBBEEDS)

##END SCRIPT
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'End Script, RunTime: '+ Functions.StrfTimeDelta(datetime.datetime.now()-tmpStartTimeScript))
del tmpScriptName, tmpStartTimeScript