# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
@create  :  20190215 (yyyyMMdd)
@author  :  Omegal Gangapersad
@Purpose :  Clean RawData table to produce a single clean table with data which can be used to perform analysis on. This is realised by walking through these steps
				1. Identify in Datastream data where columns are Error - index
				2. Identify these index in SEC
				3. Identify these index in BEE
				4. Create StagingData table
				5. Output StagingData to csv
@instruct:  Do not remove line 1 and 2. Code in this script on line 2 is used to be able to import other scripts as modules. 
@update  :  Date (yyyyMMdd)
            20120215:               
                - Created script
                - Played with plots
            20120429:   
                - Rewrote reading StagingData
                - Created segment to create working dataframe
                - Created descriptives of count observations - total and per sector
"""

##START SCRIPT
import os
import Functions
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
tmpScriptName = os.path.basename(__file__)
tmpStartTimeScript = datetime.datetime.now()
tmpScriptName = os.path.basename(__file__)
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start Script')
MainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
ExportDir = MainDir + '\\Output\\Descriptives\\' 

##READ STAGING DATA
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP1: Read StagingData') 
try:
    InputFilenameBBBEE = 'BBBEE.csv'
    InputFilenameDS = 'DS.csv'
    InputFilenameFirm = 'Firm.csv'
    InputFilenameDates = 'Dates.csv'  
    tmpMainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
    tmpImportDirectory = tmpMainDir + '\\Input\\StagingData\\' 
    StagingBBBEE = pd.read_csv(tmpImportDirectory + InputFilenameBBBEE)
    StagingDS = pd.read_csv(tmpImportDirectory + InputFilenameDS)
    StagingFirm = pd.read_csv(tmpImportDirectory + InputFilenameFirm)
    StagingDates = pd.read_csv(tmpImportDirectory + InputFilenameDates)
    del InputFilenameBBBEE, InputFilenameDS, InputFilenameFirm, InputFilenameDates, tmpMainDir, tmpImportDirectory
except:
    import StagingData


Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP2: Create working dataset called df') 
# DEFINE DF CRITERIA - CAN BE LOOPED OVER
BBBEEMonth = 4 #define month where BBBEE is released - April is value from van der Merwe paper
BBBEELag = 4 #define months Lag from BBBEE release Month - you can adjust this value to check when market reacts
MonthYearEnd = BBBEEMonth + BBBEELag #define month for year end to calculate

StagingDates.loc[(StagingDates['Month'] == BBBEEMonth) & (StagingDates['MonthEndDummy'] == 1),'BBBEEReleaseDateDummy'] = StagingDates.loc[(StagingDates['Month'] == BBBEEMonth) & (StagingDates['MonthEndDummy'] == 1),'Year'] #put year in BBBEEdummy to identify from where BBBEE scores run
StagingDates.loc[(StagingDates['Month'] == MonthYearEnd) & (StagingDates['MonthEndDummy'] == 1),'ReturnYearEndDummy'] = StagingDates.loc[(StagingDates['Month'] == MonthYearEnd) & (StagingDates['MonthEndDummy'] == 1),'Year'] #put year in BBBEEdummy to identify from where Return year end run
tmpYear = StagingDates[['DateID','ReturnYearEndDummy']]
tmpYear.columns = ['DateID','Year'] 
StagingDS = pd.merge(StagingDS,pd.DataFrame(tmpYear),on='DateID',how='left') #create column with ReturnYearEndDummy to merge with StagingBBBEE
df = StagingDS.loc[StagingDS['Year']!=0] #this will return more rows than StagingBBBEE because StagingBBBEE only capture sec year combination for which there is a BBBEE score df captures the sec even when there is no BBBEE score
df = pd.merge(df,StagingBBBEE,on=['Year','FirmID'], how='left')

ObsVariableYear = df.groupby('Year').count() #Check number of firms with BBBEE score/without BBBEE score per year
ObsSectorYearCount = df.pivot_table(['BBBEE_Rank','Price'], index='Year', columns='DS_SECTORID', aggfunc='count') #Check number of firms per sector per year - compare with JSE All share Index - Price as proxy for all observations
ObsVariableYear.to_excel(ExportDir + 'ObsVariableYear.xlsx', sheet_name='Input')
ObsSectorYearCount.to_excel(ExportDir + 'ObsSectorYearCount.xlsx', sheet_name='Input') #work in excel to create %percentages
<<<<<<< HEAD

=======
>>>>>>> 8980219135c001d6c0a493cfd082606588608efd



"""
1. Define BBBEE lag - DONE

2. Create yearly dataset
in stagingdates where monthend and month are condition fill year then BBBEE left join based on that to DS
calculate annual return ds based on this - DONE

3. Check number of firms with BBBEE score/without BBBEE score per year
x = ObsV.describe()

4. Check number of firms per sector per year - compare with JSE All share Index




#StagingDates['BBBEEReleaseDateDummy'] = StagingDates['BBBEEReleaseDateDummy'].replace(to_replace=0, method='ffill') #fill BBBEEReleaseDateDummy with value till next year end - completing year id for each date
#StagingDates['ReturnYearEndDummy'] = StagingDates['ReturnYearEndDummy'].replace(to_replace=0, method='ffill') #fill YearEnd with value till next year end - completing year id for each date

df = pd.merge(StagingBBBEE,StagingDS,on=['Year','FirmID'], how='left')
StagingBBBEE = pd.merge(StagingBBBEE,StagingDS,on=['Year','FirmID'], how='left')



StagingBBBEE = pd.merge(RawBBBEE,tmpFIRMBBBEE,on='CompanyName', how='left')

     







    
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Initiate Staging Data')
import StagingData
StagingData = StagingData.StagingData
tmpKeyData = pd.DataFrame(StagingData[['Year','LogPriceReturn', 'BBBEE_Rank','BVPS','MV']]) #Add yields eventually
FinalData = tmpKeyData.dropna() #removed all nan - this is the cleanest set of data

#DESCRIPTIVES PER YEAR
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Compute Descriptives per factor table aggregate')
DescriptiveLogPriceReturnTotalSample = FinalData['LogPriceReturn'].describe()
DescriptiveBEERankTotalSample = FinalData['BBBEE_Rank'].describe()
DescriptiveBVPSTotalSample = FinalData['BVPS'].describe()
DescriptiveMVTotalSample = FinalData['MV'].describe() 

#DESCRIPTIVES PER YEAR
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Compute Descriptives per factor table per year')
df = FinalData[['Year','LogPriceReturn']]
DescriptiveLogPriceReturnYear = df.groupby('Year').describe()
df = FinalData[['Year','BBBEE_Rank']]
DescriptiveBBBEERankYear = df.groupby('Year').describe()
df = FinalData[['Year','BVPS']]
DescriptiveBVPSYear = df.groupby('Year').describe()
df = FinalData[['Year','MV']]
DescriptiveBVPSYear = df.groupby('Year').describe()

#Create ranks LogPriceReturn, BVPS and MV


UniqueYears = pd.DataFrame(FinalData['Year'].unique())#get unique years to get ranks per year
UniqueYears.columns = ['Year']



RankData = pd.DataFrame(np.nan, index=[0], columns=['Year','LogPriceReturn_Rank','BBBEE_Rank','BVPS_Rank','MV_Rank']) #create an empty dataframe 

#call function that loops through grouping and to be ranked columns

tmpYear = UniqueYears['Year'][0]
df = FinalData.loc[FinalData['Year'] == int(tmpYear), ['Year','BBBEE_Rank','LogPriceReturn','BVPS','MV',]]
df['LogPriceReturn_Rank'] = df['LogPriceReturn'].rank() 
df['BVPS_Rank'] = df['BVPS'].rank() 
df['MV_Rank'] = df['MV'].rank() 
df2 = df[['Year','LogPriceReturn_Rank','BBBEE_Rank','BVPS_Rank','MV_Rank']].copy()

RankData = RankData.append(df2)




#SCATTERPLOTS
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Run Scatterplots')
ScatterPlotContainer = plt.figure()
ScatterPlot1 = ScatterPlotContainer.add_subplot(2,2,1)
ScatterPlot2 = ScatterPlotContainer.add_subplot(2,2,2)
ScatterPlot3 = ScatterPlotContainer.add_subplot(2,2,3)
                                                
ScatterPlot1.scatter('BBBEE_Rank','LogPriceReturn',data=FinalData)
ScatterPlot2.scatter('MV','LogPriceReturn',data=FinalData)
ScatterPlot3.scatter('BVPS','LogPriceReturn',data=FinalData)

#OUTPUT
MainDir = os.path.dirname(os.path.realpath(__file__))
ExportDir = MainDir + '\\Output\\Descriptives\\' 
FinalData.to_csv(ExportDir + 'FinalData.csv', encoding='utf-8', index=False)
plt.savefig(ExportDir + 'ScatterPlotPriceReturnVsFactors.pdf')
"""
