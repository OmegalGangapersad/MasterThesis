# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
@create  :  20190209 (yyyyMMdd)
@author  :  Omegal Gangapersad
@Purpose :  Clean RawData table to produce a single clean table with data which can be used to perform analysis on. This is realised by walking through these steps
				1. Identify in Datastream data where columns are Error - index
				2. Identify these index in SEC
				3. Identify these index in BBBEE
				4. Create StagingData table
				5. Output StagingData to csv
@instruct:  Do not remove line 1 and 2. Code in this script on line 2 is used to be able to import other scripts as modules. 
@update  :  Date (yyyyMMdd)
            20120211:               
                - Finished up until step 3 of clean rawdata, 
                - Tidy up code by intermittently deleting tmp variables
            20120212:
                - Apply available Datastream securities on BBBEE dataset - masking
                - Adjusted Datastream retrieval excel file to get yearly data
                - Standardized Datastream data
                - Calculated PriceReturn 
                - Created StagingData
            20120213:
                - Created an index for available Datastream securities and yields
                - Finished StagingData
                - Cleaned code a bit, deleting all irrelevant variables
                - Export StagingData to csv
            20120215:
                - Added Log functions
                - Cleaned structure of this script
            20120216:
                - Renamed script from CleanRawData.py to StagingData.py
                - Removed loop to in StagingData with merge (reducing run time of StagingData.py from 8mins to 1sec)
"""
##START SCRIPT
import os
import Functions
import datetime
import numpy as np
import pandas as pd
tmpScriptName = os.path.basename(__file__)
tmpStartTimeScript = datetime.datetime.now()
tmpScriptName = os.path.basename(__file__)
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start Script')


##READ RAWDATA    
import ReadRawData  
RawBBBEE = ReadRawData.RawDataBBBEE
RawPRICE = ReadRawData.RawDataPRICE
RawBVPS = ReadRawData.RawDataBVPS
RawMV = ReadRawData.RawDataMV
RawYLD = ReadRawData.RawDataYLD
RawSECDS = ReadRawData.RawDataSECDS
RawSECBBBEEDS = ReadRawData.RawDataSECBBBEEDS
	
##STEP1: Identify in Datastream orice, bvps, mv, data where columns are Error, create Staging Datastream
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP1: Identify in Datastream orice, bvps, mv, data where columns are Error, create Staging Datastream: Find Columns Error')

#FIND COLUMNS ERROR
tmpErrorStr = '$$ER: E100,INVALID CODE OR EXPRESSION ENTERED' #specific error message for Datastream
tmpErrorPRICE = (np.array(RawPRICE) == tmpErrorStr) #creates bool array where is error
tmpErrorBVPS = (np.array(RawBVPS) == tmpErrorStr)
tmpErrorMV = (np.array(RawMV) == tmpErrorStr)
tmpErrorTot = tmpErrorPRICE + tmpErrorBVPS + tmpErrorMV
ErrorRow = tmpErrorTot[0,1:] #creates 1 array with true false (true is error), concluding finding Error Data
ErrorRow2 = tmpErrorTot[0,:] 
del tmpErrorStr, tmpErrorPRICE, tmpErrorBVPS, tmpErrorMV, tmpErrorTot

#CREATE PREREQUISITE DATA STAGING DATASTREAM
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP1: Identify in Datastream orice, bvps, mv, data where columns are Error, create Staging Datastream: Create Staging Datastream')
tmpDSMat = np.array(RawPRICE)
tmpStagingDSAvailableVec =  np.tile(ErrorRow2, (tmpDSMat.shape[0],1)) 
tmpStagingDSAvailableVec2 = tmpStagingDSAvailableVec[0,:]
tmpColumnNames =np.array(RawPRICE.columns)
tmpColumnNames2 = pd.DataFrame((tmpColumnNames[tmpStagingDSAvailableVec2 == False]))
tmpColumnNames2[0][0] = 'Year'
del tmpColumnNames

#CREATE DATAFRAMES STAGING DATASTREAM
StagingPrice = pd.DataFrame(np.array(RawPRICE)[:,tmpStagingDSAvailableVec[0,:] == False])
StagingBVPS = pd.DataFrame(np.array(RawBVPS)[:,tmpStagingDSAvailableVec[0,:] == False])
StagingMV = pd.DataFrame(np.array(RawMV)[:,tmpStagingDSAvailableVec[0,:] == False])
StagingPriceReturn = StagingPrice.pct_change() #CHECK!
StagingLogPriceReturn = pd.DataFrame(np.log(1 + StagingPriceReturn))
StagingPriceReturn[0] = StagingPrice[0]
StagingLogPriceReturn[0] = StagingPrice[0]
StagingYields = pd.DataFrame(RawYLD)
StagingYields.columns = ['Year','US10YR','SA10YR','US2YR','SA2YR']

#CREATE STACKED STAGING DATASTREAM (TO (WITHOUT LOOP) MERGE IN FINAL STAGINGDATA DATAFRAME LATER)
tmpStagingDSAvailableSECID = pd.DataFrame(RawSECDS[tmpStagingDSAvailableVec2[1:] == False])
tmpStagingDSAvailableSECID = tmpStagingDSAvailableSECID.reset_index()
tmpStagingDSAvailableSECID['DSAvailableSECID'] = tmpStagingDSAvailableSECID.index.get_level_values(0)
tmpStagingDSAvailableSECID2 = tmpStagingDSAvailableSECID[['DSAvailableSECID', 'RIC']].copy()

StackPrice = Functions.StackDFDS(StagingPrice,'Price', tmpStagingDSAvailableSECID2)
StackBVPS = Functions.StackDFDS(StagingBVPS,'BVPS', tmpStagingDSAvailableSECID2)
StackMV = Functions.StackDFDS(StagingMV,'MV', tmpStagingDSAvailableSECID2)
StackPriceReturn = Functions.StackDFDS(StagingPriceReturn,'PriceReturn', tmpStagingDSAvailableSECID2)
StackLogPriceReturn = Functions.StackDFDS(StagingLogPriceReturn,'LogPriceReturn', tmpStagingDSAvailableSECID2)
del tmpDSMat,tmpStagingDSAvailableVec, tmpStagingDSAvailableVec2, tmpColumnNames2, tmpStagingDSAvailableSECID, tmpStagingDSAvailableSECID2


##STEP2: Identify Staging Datastream securities in SECDS 
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP2: Identify Staging Datastream securities in SECDS')
tmpSEC = np.array(RawSECDS) #create temporary variable to get available sec
AvailDSSEC = tmpSEC[ErrorRow == False,0] #Available Datastream securities
del tmpSEC, ErrorRow, ErrorRow2  #remove temporary variable


##STEP3: Identify Staging Datastream securities in BBBEE
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP3: Identify Staging Datastream securities in BBBEE')
tmpBBBEERICID = pd.Series(RawSECBBBEEDS.RIC)
tmpBBBEEName = pd.Series(RawSECBBBEEDS.Name)
tmpAvailSEC = pd.Series(AvailDSSEC)
AvailBBBEESECIDBool = tmpBBBEERICID.isin(tmpAvailSEC) #identify RIC of BBBEE securities list which have Datastream data available
AvailBBBEEName = tmpBBBEEName[AvailBBBEESECIDBool == True] #identify Names of BBBEE securities list which have Datastream data available
del tmpBBBEERICID,tmpAvailSEC, tmpBBBEEName


##STEP4: Create StagingData
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP4: Create StagingData')

# CREATE STAGING SECURITY TABLE
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP4: Create StagingData, Create Staging Security Table')
StagingSEC = pd.DataFrame() #create security identifier index
StagingSEC['SECID'] = pd.DataFrame(AvailDSSEC).index.tolist()
StagingSEC['RIC'] = pd.DataFrame(AvailDSSEC) #Add Name

# CREATE TMPSTAGING DATA WITH RELEVANT BBBEE INFO OF BBBEE SECURITIES
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP4: Create StagingData, Create tmpStagingData with relevant BBBEE info of BBBEE securities')
tmpStagingData = np.array(RawBBBEE)[:,2:]
tmpDF = pd.DataFrame(RawSECBBBEEDS[['Name', 'RIC']].copy()).drop_duplicates() #source: http://www.datasciencemadesimple.com/get-unique-values-rows-dataframe-python-pandas/
tmpStagingDataNamesList = pd.Series(np.array(tmpStagingData)[:,0])
tmpStagingDataNamesAvailableVec = tmpStagingDataNamesList.isin(AvailBBBEEName)
tmpStagingDataNamesAvailableVec2 =  np.tile(tmpStagingDataNamesAvailableVec, (tmpStagingData.shape[1],1)) # use np.tile to repeat the vector for the number of columns in tmpStagingDataNamesList - see here: https://stackoverflow.com/questions/1550130/cloning-row-or-column-vectors
tmpStagingDataNamesAvailableMat = tmpStagingDataNamesAvailableVec2.transpose()
StagingData = pd.DataFrame(tmpStagingData[tmpStagingDataNamesAvailableMat[:,0] == True,:])
StagingData.columns = ['Name', 'Year', 'BBBEE_Rank','BBBEE_Score','Ownership_Score','Management_Score','EmploymentEquity_Score','SkillsDevelopment_Score','PreferentialProcurement_Score','EnterpriseDevlopment_Score','SocioEconomicDevelopment_Score']
StagingData = pd.merge(StagingData, tmpDF, on='Name', how='left') #Add RIC Codes from RAWSECBBBEEDS, merge is SQL like join - useful alternative for loops
StagingData = pd.merge(StagingData, StagingSEC, on='RIC', how='left') #Add SECID from StagingSEC
StagingData = pd.merge(StagingData, StackPrice, on=['RIC', 'Year'], how='left')
StagingData = pd.merge(StagingData, StackPriceReturn, on=['RIC', 'Year'], how='left')
StagingData = pd.merge(StagingData, StackLogPriceReturn, on=['RIC', 'Year'], how='left')
StagingData = pd.merge(StagingData, StackBVPS, on=['RIC', 'Year'], how='left')
StagingData = pd.merge(StagingData, StackMV, on=['RIC', 'Year'], how='left')
StagingData = pd.merge(StagingData, StagingYields, on='Year', how='left')
del AvailBBBEESECIDBool, AvailBBBEEName, AvailDSSEC, tmpStagingDataNamesList, tmpStagingDataNamesAvailableMat, tmpStagingDataNamesAvailableVec, tmpStagingDataNamesAvailableVec2 #delete tmp variables
#Add statistic to show which BBBEE names are not available in Datastream


##STEP5: Output StagingData to csv
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP5: Output StagingData to csv')
MainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
ExportDir = MainDir + '\\Input\\StagingData\\' 
StagingPrice.to_csv(ExportDir + 'PRICE.csv', encoding='utf-8', index=False)
StagingPriceReturn.to_csv(ExportDir + 'PRICERETURN.csv', encoding='utf-8', index=False)
StagingBVPS.to_csv(ExportDir + 'BVPS.csv', encoding='utf-8', index=False)
StagingMV.to_csv(ExportDir + 'MV.csv', encoding='utf-8', index=False)
StagingSEC.to_csv(ExportDir + 'SEC.csv', encoding='utf-8', index=False)
StagingData.to_csv(ExportDir + 'Data.csv', encoding='utf-8', index=False)
del MainDir, ExportDir, RawBBBEE, RawBVPS, RawMV, RawPRICE, RawSECBBBEEDS, RawSECDS, RawYLD, StackBVPS, StackLogPriceReturn, StackMV, StackPrice, StackPriceReturn, tmpDF, tmpStagingData


##END SCRIPT
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'End Script, RunTime: '+ Functions.StrfTimeDelta(datetime.datetime.now()-tmpStartTimeScript))
del tmpScriptName, tmpStartTimeScript