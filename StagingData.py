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
            20190211:               
                - Finished up until step 3 of clean rawdata, 
                - Tidy up code by intermittently deleting tmp variables
            20190212:
                - Apply available Datastream securities on BBBEE dataset - masking
                - Adjusted Datastream retrieval excel file to get yearly data
                - Standardized Datastream data
                - Calculated PriceReturn 
                - Created StagingData
            20190213:
                - Created an index for available Datastream securities and yields
                - Finished StagingData
                - Cleaned code a bit, deleting all irrelevant variables
                - Export StagingData to csv
            20190215:
                - Added Log functions
                - Cleaned structure of this script
            20190216:
                - Renamed script from CleanRawData.py to StagingData.py
                - Removed loop to in StagingData with merge (reducing run time of StagingData.py from 8mins to 1sec)
            20190425:
                - Considered to add constituents that represent the wider market to create market based proxies, by using JSE All Share Index, however overlap was quite big as of latest JSE All Share Index holding and history constituents JSE All Share Index was not available
                - I need to reformat the date column to accomodate for different lags
                - Structure from DS (most data) only removed columns with no value
            20190426:    
                - Restructured the Error finding code, as it did not capture all errors - code slowed significantly
                - Restructered the StagingPrice, MV BVPS etc. to make it simpler
                - Added BP
                - I noticed this script downloads the Rawdata each time rather than  access from local disk
                - Added minimum shareprice to correct for penny stock
                - Added DateID, corrected SECID
            20190427:
                - Further cleaned SECID
                - Added cumulative returns
                - Rewrote Stacking DS
                
"""
##START SCRIPT

import os
import Functions
import datetime
import numpy as np
import math
from numpy import matlib as mb
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
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP1: Identify in Datastream price, bvps, mv, data where columns are Error, create Staging Datastream: Find Columns Error')

#CREATE DATES DATAFRAMEWORK
StagingDates= Functions.DatesDF(RawPRICE['Name'])

#REMOVE DATES COLUMN FROM DS DATA
StagingPrice = np.array(RawPRICE)[:,1:] #select set of data without date
StagingPrice = pd.DataFrame(StagingPrice)
StagingPrice[StagingPrice==0] = 0.01
StagingPrice = np.array(StagingPrice)
StagingBVPS = np.array(RawBVPS)[:,1:]
StagingMV = np.array(RawMV)[:,1:]

#FIND AND REMOVE COLUMNS ERROR AND PENNY STOCKS
tmpErrorStr = '$$ER'
tmpMinSharePrice = 0.01
tmpMaxTimeFail = math.ceil(0.1*StagingPrice.shape[0])
ErrorRow = np.zeros(shape=StagingPrice.shape[1])
PennyRow = np.zeros(shape=StagingPrice.shape[1])
for ii in range(StagingPrice.shape[1]):
    tmpStrValues = np.array([str(StagingPrice[0,ii]),str(StagingBVPS[0,ii]),str(StagingMV[0,ii])]) 
    tmpFalseValues = np.array(np.array(np.char.find(tmpStrValues, tmpErrorStr) == -1))[np.array(np.char.find(tmpStrValues, tmpErrorStr) != -1)] #np.char.find returns a value unequal to -1 if any of the values in the tmpArray contains '$$ER', here I ask to return a array where the values are unequal to -1 (so return the array which contains all the errors)     
    if tmpFalseValues.shape[0] >0: 
        ErrorRow[ii] = True
    else:
        ErrorRow[ii] = False
        tmpPriceFailSum = np.transpose([(StagingPrice[:,ii] < tmpMinSharePrice)])
        tmpPriceFailCount = np.sum(tmpPriceFailSum, axis=0)
        if tmpPriceFailCount > tmpMaxTimeFail:
            PennyRow[ii] = True
        else:
            PennyRow[ii] = False
del tmpStrValues,tmpFalseValues,ii
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP1: Identify in Datastream price, bvps, mv, data where columns are Error, create Staging Datastream: Finished Columns Error')
TotErrorRow = ErrorRow + PennyRow

StagingPrice = pd.DataFrame(StagingPrice[:,TotErrorRow == False]) 
StagingBVPS = pd.DataFrame(StagingBVPS[:,TotErrorRow == False])
StagingBP = pd.DataFrame(np.divide(np.array(StagingBVPS),np.array(StagingPrice)))
StagingMV = pd.DataFrame(StagingMV[:,TotErrorRow == False])
StagingPriceReturn = StagingPrice.pct_change()
StagingPriceReturnCum = (1+StagingPriceReturn).cumprod() -1
StagingPriceLogReturn = pd.DataFrame(np.log(1 + StagingPriceReturn))
StagingPriceLogReturnCum = (1+StagingPriceReturn).cumprod() -1

#CREATE SECID FRAMEWORK
StagingSECID = pd.DataFrame(RawSECDS[TotErrorRow == False])
StagingSECID =StagingSECID.reset_index()
StagingSECID['SECID'] = StagingSECID.index.get_level_values(0)
StagingSECID.columns = ['Raw_SECID','RIC','ISIN','CompanyName','SECID']

#CREATE STACKING DATA
StackPrice = Functions.StackDFDS_simple(StagingPrice,'Price') 
StackBVPS = Functions.StackDFDS_simple(StagingBVPS,'BVPS')
StackBP = Functions.StackDFDS_simple(StagingBP,'BP')
StackMV = Functions.StackDFDS_simple(StagingMV,'MV')
StackPriceReturn = Functions.StackDFDS_simple(StagingPriceReturn,'PriceReturn')
StackPriceReturnCum = Functions.StackDFDS_simple(StagingPriceReturnCum,'PriceReturnCum')
StackPriceLogReturn = Functions.StackDFDS_simple(StagingPriceLogReturn,'PriceLogReturn')
StackPriceLogReturnCum = Functions.StackDFDS_simple(StagingPriceLogReturnCum,'PriceLogReturnCum')

StackDS = pd.DataFrame({
                        'DateID':np.array(StackPrice['DateID']),
                        'SECID':np.array(StackPrice['SECID'])                        
                        })
StackDS =  pd.merge(StackDS,StackPrice, on=['SECID', 'DateID'], how='left')    
StackDS =  pd.merge(StackDS,StackBVPS, on=['SECID', 'DateID'], how='left')
StackDS =  pd.merge(StackDS,StackBP, on=['SECID', 'DateID'], how='left')
StackDS =  pd.merge(StackDS,StackMV, on=['SECID', 'DateID'], how='left')
StackDS =  pd.merge(StackDS,StackPriceReturn, on=['SECID', 'DateID'], how='left')
StackDS =  pd.merge(StackDS,StackPriceReturnCum, on=['SECID', 'DateID'], how='left')
StackDS =  pd.merge(StackDS,StackPriceLogReturn, on=['SECID', 'DateID'], how='left')
StackDS =  pd.merge(StackDS,StackPriceLogReturnCum, on=['SECID', 'DateID'], how='left')

#ADD YIELDS TO STACKDS
StagingYLD = RawYLD
StagingYLD.columns = ['DateTime','US10YR','SA10YR','US2YR','SA2YR']
StagingYLD = pd.merge(StagingYLD,StagingDates,on=['DateTime'],how='left')
StagingYLD = pd.DataFrame({
                            'DateID': np.array(StagingYLD['DateID']),
                            'SA10YR': np.array(StagingYLD['SA10YR']),
                            'SA2YR': np.array(StagingYLD['SA2YR']),
                            'US10YR': np.array(StagingYLD['US10YR']),
                            'US2YR': np.array(StagingYLD['US2YR']),
                         })        
StackDS = pd.merge(StackDS,StagingYLD, on='DateID', how='left')    

#CREATE STAGING BBBEE
tmpSECBBBEE = pd.DataFrame({
                            'CompanyName': np.array(RawSECBBBEEDS['Name']),
                            'RIC': np.array(RawSECBBBEEDS['RIC']),
                            })
tmpSECBBBEE = tmpSECBBBEE.drop_duplicates() #get unique values of companyname and RIC to prevent bloat in merge below line
StagingBBBEE = pd.merge(RawBBBEE,tmpSECBBBEE,on='CompanyName', how='left') #get RIC into StagingBBBEE
StagingBBBEE = pd.merge(StagingBBBEE,StagingSECID,on='RIC',how='left') #get SECID into StagingBBBEE
StagingBBBEE = pd.DataFrame({
                            'Year': np.array(StagingBBBEE['Year']),
                            'SECID': np.array(StagingBBBEE['SECID']),
                            'BBBEE_Rank': np.array(StagingBBBEE['Rank']),
                            'BBBEE_Score': np.array(StagingBBBEE['Score_BEE']),
                            'BBBEE_OWN': np.array(StagingBBBEE['Score_Ownership']),
                            'BBBEE_MAN': np.array(StagingBBBEE['Score_Management']),
                            'BBBEE_EMP': np.array(StagingBBBEE['Score_EmploymentEquity']),
                            'BBBEE_SKL': np.array(StagingBBBEE['Score_SkillsDevelopment']),                       
                            'BBBEE_PRF': np.array(StagingBBBEE['Score_PreferentialProcurement']), 
                            'BBBEE_ENT': np.array(StagingBBBEE['Score_EnterpriseDevelopment']), 
                            'BBBEE_SOC': np.array(StagingBBBEE['Score_SocioEconomicDevelopment'])                       
                            })
StagingBBBEE = StagingBBBEE[['Year','SECID','BBBEE_Rank','BBBEE_Score','BBBEE_OWN','BBBEE_MAN','BBBEE_EMP','BBBEE_SKL','BBBEE_PRF','BBBEE_ENT','BBBEE_SOC']] #force order


"""
#CREATE PREREQUISITE DATA STAGING DATASTREAM
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP1: Identify in Datastream price, bvps, mv, data where columns are Error, create Staging Datastream: Create Staging Datastream')
tmpDSMat = np.array(RawPRICE)
tmpStagingDSAvailableVec =  np.tile(ErrorRow2, (tmpDSMat.shape[0],1)) 
tmpStagingDSAvailableVec2 = tmpStagingDSAvailableVec[0,:]
tmpColumnNames =np.array(RawPRICE.columns)
tmpColumnNames2 = pd.DataFrame((tmpColumnNames[tmpStagingDSAvailableVec2 == False]))
tmpColumnNames2[0][0] = 'Year' 
del tmpColumnNames


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
"""