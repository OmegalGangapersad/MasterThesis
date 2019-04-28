# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
@create  :  20190209 (yyyyMMdd)
@author  :  Omegal Gangapersad
@Purpose :  Clean RawData table to produce a single clean table with data which can be used to perform analysis on. This is realised by walking through these steps
				1. Create Dates table called StagingDates - in which all dates are included, including their respective id, year, month, day, month end etc
				2. Create StagingDS arrays - first create clean Staging Datastream Data (price,bvps,mv etc.)
				3. Create FirmID table - in which all firms are included - including their id, name, ric etc.
				4. Create StackDS - reshape the StagingDS arrays to make them more easily fittable with BBBEE data
				5. Create StagingBBBEE - create clean BBBEE table with compatible ids to make them more easily fittable with DS data
              6. Output
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
                - Added DateID, corrected FirmID
            20190427:
                - Replaced SECID with FirmID
                - Further cleaned FirmID
                - Added cumulative returns
                - Rewrote Stacking DS
                - Rewrote YLD to add to StackDS
                - Rewrote Staging BBBBEE
                
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
	

##STEP1: CREATE STAGINGDATES
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP1: Create StagingDates')
StagingDates = Functions.DatesDF(RawPRICE['Name'])


##STEP2: CREATE STAGINGDS ARRAYS
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP2: Create StagingDates')

#REMOVE DATES COLUMN FROM DS DATA
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP2: Create StagingDates - REMOVE DATES COLUMN FROM DS DATA')
StagingPrice = np.array(RawPRICE)[:,1:] #select set of data without date
StagingPrice = pd.DataFrame(StagingPrice)
StagingPrice[StagingPrice==0] = 0.01
StagingPrice = np.array(StagingPrice)
StagingBVPS = np.array(RawBVPS)[:,1:]
StagingMV = np.array(RawMV)[:,1:]

#FIND AND REMOVE COLUMNS ERROR AND PENNY STOCKS
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP2: Create StagingDates - FIND AND REMOVE COLUMNS ERROR AND PENNY STOCKS')
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
TotErrorRow = ErrorRow + PennyRow

StagingPrice = pd.DataFrame(StagingPrice[:,TotErrorRow == False]) 
StagingBVPS = pd.DataFrame(StagingBVPS[:,TotErrorRow == False])
StagingBP = pd.DataFrame(np.divide(np.array(StagingBVPS),np.array(StagingPrice)))
StagingMV = pd.DataFrame(StagingMV[:,TotErrorRow == False])
StagingPriceReturn = StagingPrice.pct_change()
StagingPriceReturnCum = (1+StagingPriceReturn).cumprod() -1
StagingPriceLogReturn = pd.DataFrame(np.log(1 + StagingPriceReturn))
StagingPriceLogReturnCum = (1+StagingPriceReturn).cumprod() -1


##STEP3: CREATE STAGINGFIRMID 
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP3: Create StagingFirmID')
StagingFirmID = pd.DataFrame(RawSECDS[TotErrorRow == False])
StagingFirmID =StagingFirmID.reset_index()
StagingFirmID['FirmID'] = StagingFirmID.index.get_level_values(0)
StagingFirmID.columns = ['Raw_FirmID','RIC','ISIN','CompanyName','FirmID']


##STEP4: REATE STACKDS
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP4: Create StackDS')
StackPrice = Functions.StackDFDS_simple(StagingPrice,'Price') 
StackBVPS = Functions.StackDFDS_simple(StagingBVPS,'BVPS')
StackBP = Functions.StackDFDS_simple(StagingBP,'BP')
StackMV = Functions.StackDFDS_simple(StagingMV,'MV')
StackPriceReturn = Functions.StackDFDS_simple(StagingPriceReturn,'PriceReturn')
StackPriceReturnCum = Functions.StackDFDS_simple(StagingPriceReturnCum,'PriceReturnCum')
StackPriceLogReturn = Functions.StackDFDS_simple(StagingPriceLogReturn,'PriceLogReturn')
StackPriceLogReturnCum = Functions.StackDFDS_simple(StagingPriceLogReturnCum,'PriceLogReturnCum')

StagingDS = pd.DataFrame({
                        'DateID':np.array(StackPrice['DateID']),
                        'FirmID':np.array(StackPrice['FirmID'])                        
                        })
StagingDS =  pd.merge(StagingDS,StackPrice, on=['FirmID', 'DateID'], how='left')    
StagingDS =  pd.merge(StagingDS,StackBVPS, on=['FirmID', 'DateID'], how='left')
StagingDS =  pd.merge(StagingDS,StackBP, on=['FirmID', 'DateID'], how='left')
StagingDS =  pd.merge(StagingDS,StackMV, on=['FirmID', 'DateID'], how='left')
StagingDS =  pd.merge(StagingDS,StackPriceReturn, on=['FirmID', 'DateID'], how='left')
StagingDS =  pd.merge(StagingDS,StackPriceReturnCum, on=['FirmID', 'DateID'], how='left')
StagingDS =  pd.merge(StagingDS,StackPriceLogReturn, on=['FirmID', 'DateID'], how='left')
StagingDS =  pd.merge(StagingDS,StackPriceLogReturnCum, on=['FirmID', 'DateID'], how='left')

#ADD YIELDS TO STACKDS
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP4: Create StackDS - ADD YIELDS TO STACKDS')
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
StagingDS = pd.merge(StagingDS,StagingYLD, on='DateID', how='left')    


##STEP5: CREATE STAGING BBBEE
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP5: Create StagingBBBEE')
tmpSECBBBEE = pd.DataFrame({
                            'CompanyName': np.array(RawSECBBBEEDS['Name']),
                            'RIC': np.array(RawSECBBBEEDS['RIC']),
                            })
tmpSECBBBEE = tmpSECBBBEE.drop_duplicates() #get unique values of companyname and RIC to prevent bloat in merge below line
StagingBBBEE = pd.merge(RawBBBEE,tmpSECBBBEE,on='CompanyName', how='left') #get RIC into StagingBBBEE
StagingBBBEE = pd.merge(StagingBBBEE,StagingFirmID,on='RIC',how='left') #get FirmID into StagingBBBEE
StagingBBBEE = pd.DataFrame({
                            'Year': np.array(StagingBBBEE['Year']),
                            'FirmID': np.array(StagingBBBEE['FirmID']),
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
StagingBBBEE = StagingBBBEE[['Year','FirmID','BBBEE_Rank','BBBEE_Score','BBBEE_OWN','BBBEE_MAN','BBBEE_EMP','BBBEE_SKL','BBBEE_PRF','BBBEE_ENT','BBBEE_SOC']] #force order


##STEP6: OUTPUT
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP6: Output')
MainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
ExportDir = MainDir + '\\Input\\StagingData\\' 
StagingDates.to_csv(ExportDir + 'Dates.csv', encoding='utf-8', index=False)
StagingFirmID.to_csv(ExportDir + 'Firm.csv', encoding='utf-8', index=False)
StagingDS.to_csv(ExportDir + 'DS.csv', encoding='utf-8', index=False)
StagingBBBEE.to_csv(ExportDir + 'BBBEE.csv', encoding='utf-8', index=False)


##END SCRIPT
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'End Script, RunTime: '+ Functions.StrfTimeDelta(datetime.datetime.now()-tmpStartTimeScript))
del tmpScriptName, tmpStartTimeScript