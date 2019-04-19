# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
@create  :	20190209 (yyyyMMdd)
@author  :  Omegal Gangapersad
@Purpose :  This is the main script for the research on BBBEE and profitability. It consists of these sub scripts:
				- Read RawData
				- Clean RawData
				- Descriptives
				- Regressions
				- Visualizations
@instruct:  Do not remove line 1 and 2. Code in this script on line 2 is used to be able to import other scripts as modules.  
            In IPython, prior to starting the script ensure variables in Spyder are cleared.
            You can accomplish this by %reset in IPython console or https://stackoverflow.com/questions/45853595/spyder-clear-variable-explorer-along-with-variables-from-memory
@update  : Date (yyyyMMdd)
		   20190215
			   - Added the CleanRawData module
			   - Tested the Main script
			   - Added log function
			   - Cleaned structure of this script
 """
##START SCRIPT
import os
import Functions
import datetime
tmpScriptName = os.path.basename(__file__)
tmpStartTimeScript = datetime.datetime.now()
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start Script')

##READ RAWDATA
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Initiate ReadRawData')
import ReadRawData
ImportRawDataDummy = True
class RawData:
    BBBEE = ReadRawData.RawDataBBBEE
    PRICE = ReadRawData.RawDataPRICE
    BVPS = ReadRawData.RawDataBVPS
    MV = ReadRawData.RawDataMV
    YLD = ReadRawData.RawDataYLD
    SECDS = ReadRawData.RawDataSECDS
    SECBBBEEDS = ReadRawData.RawDataSECBBBEEDS

##CLEAN RAWDATA
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Clean RawData')
import StagingData
class StagingData:
    Data = StagingData.StagingData
    PRICE = StagingData.StagingPrice
    PRICERETURN = StagingData.StagingPriceReturn
    BVPS = StagingData.StagingBVPS
    MV = StagingData.StagingMV
    SEC = StagingData.StagingSEC
ImportStagingData = True

##RUN DESCRIPTIVES
import Descriptives
##RUN REGRESSIONS

##END SCRIPT
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'End Script, RunTime: '+ Functions.StrfTimeDelta(datetime.datetime.now()-tmpStartTimeScript))
del tmpScriptName, tmpStartTimeScript