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
            20190215:               
                - Created script
                - Played with plots
            20190429:   
                - Rewrote reading StagingData
                - Created segment to create working dataframe
                - Created descriptives of count observations - total and per sector
            20190430:
                - Created annual return fir, riskfree, market
                - Created dataset ready for (regression) analysis
                - Identified a method to cap outliers
            20190501:
                - Finished outliers - and export thereof - next add multiple years. Found positive (low R2 Annual LogReturn and BBBEE score!)
                - Rerank BBBEE to adjust for missing data
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

# Calculate Yearly return and market premium
y = df.pivot_table('PriceLogReturnCum', index='Year', columns='FirmID')
y = y.fillna(0) #replace nan with 0 to be able to calculate yearly return
YearlyLogReturn = pd.DataFrame(np.array((1+y)/(1+ y.shift(1))-1)) #calculate yearly return
MarketLogReturn = np.nanmean(np.array(YearlyLogReturn), axis=1) #market return is the average return of all observations
YearlyLogReturn[YearlyLogReturn == 0] = float('nan')
FirmLogReturn = pd.DataFrame(YearlyLogReturn.stack())
FirmLogReturn.columns = ['FirmLogReturn']
FirmLogReturn['dfIndex'] = FirmLogReturn.index.get_level_values(0)
FirmLogReturn['FirmID'] = FirmLogReturn.index.get_level_values(1)
y = y.reset_index() #get year as column
y['dfIndex'] = y.index.get_level_values(0) #get row id as column
tmpYear = y[['Year','dfIndex']] # established mapping year and row id
FirmLogReturn = pd.merge(FirmLogReturn,tmpYear,on='dfIndex',how='left')
df = pd.merge(df,FirmLogReturn,on=['Year','FirmID'],how='left')

RiskFreeReturn = df[['Year','SA10YR']]
RiskFreeReturn = RiskFreeReturn.drop_duplicates()
RiskFreeReturn['SA10YR'] = RiskFreeReturn['SA10YR']/100
RiskFreeReturn.columns = ['Year','RiskFreeReturn']
MarketPremium = MarketLogReturn - RiskFreeReturn['RiskFreeReturn']
MarketPremium = pd.DataFrame(MarketPremium)
MarketPremium = MarketPremium.reset_index(drop=True)
MarketPremium['Year'] = tmpYear['Year']
MarketPremium.columns = ['MarketPremium','Year']
MarketLogReturn = pd.DataFrame(MarketLogReturn)
MarketLogReturn = MarketLogReturn.reset_index(drop=True)
MarketLogReturn['Year'] = tmpYear['Year']
MarketLogReturn.columns = ['MarketLogReturn','Year']

BBBEEStartYear = 2004
Dataset = df[['Year','FirmID','FirmLogReturn','BP','SIZE','BBBEE_Rank']]
Dataset = pd.merge(Dataset, MarketPremium, on='Year',how='left')
Dataset = pd.merge(Dataset, RiskFreeReturn, on='Year',how='left')
Dataset = Dataset.loc[(Dataset['Year']>=BBBEEStartYear)]
Dataset.to_excel(ExportDir + 'Dataset.xlsx', sheet_name='Input') 

#get describe of 2nd layer dataset
ObsSectorYearCount2 = Dataset.groupby('Year').count()
MinBBBEECount = round(ObsSectorYearCount2['BBBEE_Rank'].min(),-1) # find lowest number of observations BBBEE Rank rounded on nearest 10

Dataset2 = Dataset 
#adjust ranking
tmpDataset2Years = Dataset2['Year'].unique()
tmpCleanRank = Dataset2.groupby('Year')['BBBEE_Rank'].rank(ascending=True) #rerank BBBEE based on year
Dataset2['BBBEE_Rank'] = tmpCleanRank
Dataset2.loc[(Dataset2['BBBEE_Rank'] > MinBBBEECount),'BBBEE_Rank'] = float('nan')
ObsVariableYearCount3 = Dataset2.groupby('Year').count()
Dataset2.to_excel(ExportDir + 'Dataset2.xlsx', sheet_name='Input') 

OverviewIncNaN = Dataset.describe(include = [np.number]) #pre outlier overview

Dataset3 = Dataset2[['Year','FirmID','BBBEE_Rank','RiskFreeReturn','MarketPremium']]
Dataset3 = Dataset3.reset_index(drop=True)
tmpColumns = ['FirmLogReturn','BP','SIZE']

for ii in range(len(tmpColumns)):
    inpColumn = str(tmpColumns[ii])
    tmpDF1 = Functions.CapOutliers(Dataset2,inpColumn)
    Dataset3 = pd.merge(Dataset3,tmpDF1,on=['Year','FirmID'],how='left')
    

Dataset3.to_excel(ExportDir + 'Dataset3.xlsx', sheet_name='Input') 
"""


    InputDataFrame = Dataset2
    InputColumn = inpColumn
    
    
    tmpDF = InputDataFrame[['Year','FirmID',InputColumn]]
    tmpDF = tmpDF.dropna()
    tmpPreOutlierOverview = tmpDF.describe()
    tmpPreOutlierOverview.to_excel(ExportDir + InputColumn + 'PreOutlierOverview.xlsx', sheet_name='Input') 
    tmpPreOutlierPlot = sns.distplot(np.array(tmpDF[InputColumn]))  # getDistribution of InputColumn
    tmpPreOutlierPlot = tmpPreOutlierPlot.get_figure() #save distribution of InputColumn
    tmpPreOutlierPlot.savefig(ExportDir + InputColumn + '_PreOutlierDistribution.png')
    minValue = tmpPreOutlierOverview[InputColumn]['mean'] - (2*tmpPreOutlierOverview[InputColumn]['std']) #handle outlier minimum
    maxValue = tmpPreOutlierOverview[InputColumn]['mean'] + (2*tmpPreOutlierOverview[InputColumn]['std']) #handle outlier max
    tmpDF.loc[(tmpDF[InputColumn]<=minValue),InputColumn] = minValue
    tmpDF.loc[(tmpDF[InputColumn]>=maxValue),InputColumn] = maxValue
    tmpPostOutlierPlot = sns.distplot(np.array(tmpDF[InputColumn]))  # getDistribution of InputColumn
    tmpPostOutlierPlot = tmpPostOutlierPlot.get_figure() #save distribution of InputColumn
    tmpPostOutlierPlot.savefig(ExportDir + InputColumn + '_PostOutlierDistribution.png')
    tmpPostOutlierOverview = tmpDF.describe()
    tmpPostOutlierOverview.to_excel(ExportDir + InputColumn + 'PostOutlierOverview.xlsx', sheet_name='Input') 
   
    
    tmpDF1 = tmpDF
    tmpDF1 = tmpDF1.reset_index(drop=True)
    
    tmpDF1 = Functions.CapOutliers(Dataset2,inpColumn)
    Dataset3 = pd.merge(Dataset3,tmpDF1,on=['Year','FirmID'],how='left')

    tmpDF2 = tmpDF1.drop_duplicates()
    Dataset3 = pd.merge(Dataset3,tmpDF2,on=['Year','FirmID'],how='left')
    
    
#cap outliers - nans
import seaborn as sns
import pandas as pd
import os
InputColumn = 'FirmLogReturn'
InputDataframe = Dataset2
MainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
ExportDir = MainDir + '\\Output\\Descriptives\\' 

tmpDF = InputDataframe[['Year','FirmID',InputColumn]]
tmpDF = tmpDF.dropna()
tmpPreOutlierOverview = tmpDF.describe()
tmpPreOutlierPlot = sns.distplot(np.array(tmpDF[InputColumn]))  # getDistribution of InputColumn
tmpPreOutlierPlot = tmpPreOutlierPlot.get_figure() #save distribution of InputColumn
tmpPreOutlierPlot.savefig(ExportDir + InputColumn + '_PreOutlierDistribution.png')
minValue = tmpPreOutlierOverview[InputColumn]['mean'] - (2*tmpPreOutlierOverview[InputColumn]['std']) #handle outlier minimum
maxValue = tmpPreOutlierOverview[InputColumn]['mean'] + (2*tmpPreOutlierOverview[InputColumn]['std']) #handle outlier max
tmpDF.loc[(tmpDF[InputColumn]<=minValue)] = minValue
tmpDF.loc[(tmpDF[InputColumn]>=maxValue)] = maxValue
    


tmpFirmReturn = Dataset[['Year','FirmID','FirmLogReturn']] 
tmpFirmReturn = tmpFirmReturn.dropna()
OverviewFirmReturn = tmpFirmReturn.describe()
Distribution_FirmLogReturn = sns.distplot(np.array(tmpFirmReturn['FirmLogReturn']))  # getDistribution of FirmReturn
Distribution_FirmLogReturn = Distribution_FirmLogReturn.get_figure() #save distribution of FirmReturn
Distribution_FirmLogReturn.savefig(ExportDir + 'Distribution_FirmLogReturn.png')             #save distribution of FirmReturn



tmpBP = Dataset[['Year','FirmID','BP']] #add new tmpBP to new dataset
tmpBP = tmpBP.dropna()
OverviewBP = tmpBP.describe()
Distribution_BP = sns.distplot(np.array(tmpBP['BP']))  # getDistribution of FirmReturn
Distribution_BP = Distribution_BP.get_figure() #save distribution of FirmReturn
Distribution_BP.savefig(ExportDir + 'Distribution_BP.png')             #save distribution of FirmReturn
minBP = OverviewBP['BP']['mean'] - (2*OverviewBP['BP']['std']) #handle outlier minimum
maxBP = OverviewBP['BP']['mean'] + (2*OverviewBP['BP']['std']) #handle outlier max
tmpBP.loc[(tmpBP['BP']<=minBP)] = minBP
tmpBP.loc[(tmpBP['BP']>=maxBP)] = maxBP
OverviewBP2 = tmpBP.describe()
minFirmReturn = OverviewFirmReturn['FirmLogReturn']['25%'] #handle outlier minimum
maxFirmReturn = OverviewFirmReturn['FirmLogReturn']['75%'] #handle outlier minimum

#NEXT STEPS: CREATE A FUNCTION FOR CREATING DESCRIBES AND HANDLE OUTLIERS




#notes
df2[cols] = df2[cols].replace({'0':np.nan, 0:np.nan}
z = pd.DataFrame(df.columns)
p = df.loc[(df['FirmID'] == 5)]
np.mean(a, axis=0)
x = np.array(YearlyLogReturn)
z = 

x = YearlyLogReturn.groupby('Year').mean()
x = pd.DataFrame(YearlyLogReturn.mean())
x = df.describe()

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
