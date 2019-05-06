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
            20190502:
                - Cleaned code
                - Started methodology for return on different time frame
            20190503:
                - Added Riskfree return over different time frames
                - Started on factor returns
                - Abstracted Factor and Year
                - Created dummy top bottom 3iles BP, SIZE
                - Finished return on different time frame
                - Started on descriptives and scatterplot/regression/correlationmatrix per year
                - Finished descriptives (describe) and regression per year
                - Cleaned code
            20190504:
                - Finished regressions methodology and non outlier adjusted models
                - Flipped BBBEE Rank - so that higher BBBEE rank is better BBBEE. This improves intuition in regression and scatterplot
                - Abstracted regression for 5 years into function Regression5YearOutput
                - Created Abstracted scatterplots into PriceLogScatterplots
                - Started with outlier adjusted analysis
                - Finished outlier adjusted analysis
                - Cleaned code
            20190505:
                - Added comparison to JSE sector
            20190506:
                - Removed all rows with no BBBEE value in dataset1 before describe1
"""

##START SCRIPT
import os
import Functions
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from statsmodels.iolib.summary2 import summary_col
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
    InputFilenameJSE = 'JSE.csv'  
    tmpMainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
    tmpImportDirectory = tmpMainDir + '\\Input\\StagingData\\' 
    StagingBBBEE = pd.read_csv(tmpImportDirectory + InputFilenameBBBEE)
    StagingDS = pd.read_csv(tmpImportDirectory + InputFilenameDS)
    StagingFirm = pd.read_csv(tmpImportDirectory + InputFilenameFirm)
    StagingDates = pd.read_csv(tmpImportDirectory + InputFilenameDates)
    StagingJSE = pd.read_csv(tmpImportDirectory + InputFilenameJSE)
    del InputFilenameBBBEE, InputFilenameDS, InputFilenameFirm, InputFilenameDates, tmpMainDir, tmpImportDirectory
except:
    import StagingData


Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP2: Create working dataset called df') 
# DEFINE DF CRITERIA - CAN BE LOOPED OVER
BBBEEMonth = 4 #define month where BBBEE is released - April is value from van der Merwe paper
BBBEELag = 4 #define months Lag from BBBEE release Month - you can adjust this value to check when market reacts
MonthYearEnd = BBBEEMonth + BBBEELag #define month for year end to calculate

# Create Dataset0 rawest data, grouped by year
StagingDates.loc[(StagingDates['Month'] == BBBEEMonth) & (StagingDates['MonthEndDummy'] == 1),'BBBEEReleaseDateDummy'] = StagingDates.loc[(StagingDates['Month'] == BBBEEMonth) & (StagingDates['MonthEndDummy'] == 1),'Year'] #put year in BBBEEdummy to identify from where BBBEE scores run
StagingDates.loc[(StagingDates['Month'] == MonthYearEnd) & (StagingDates['MonthEndDummy'] == 1),'ReturnYearEndDummy'] = StagingDates.loc[(StagingDates['Month'] == MonthYearEnd) & (StagingDates['MonthEndDummy'] == 1),'Year'] #put year in BBBEEdummy to identify from where Return year end run
tmpYear = StagingDates[['DateID','ReturnYearEndDummy']]
tmpYear.columns = ['DateID','Year'] 
StagingDS = pd.merge(StagingDS,pd.DataFrame(tmpYear),on='DateID',how='left') #create column with ReturnYearEndDummy to merge with StagingBBBEE
Dataset0 = StagingDS.loc[StagingDS['Year']!=0] #this will return more rows than StagingBBBEE because StagingBBBEE only capture sec year combination for which there is a BBBEE score Dataset0 captures the sec even when there is no BBBEE score
Dataset0 = pd.merge(Dataset0,StagingBBBEE,on=['Year','FirmID'], how='left')

ObsVariableYear = Dataset0.groupby('Year').count() #Check number of firms with BBBEE score/without BBBEE score per year
ObsSectorYearCount = Dataset0.pivot_table(['BBBEE_Rank','Price'], index='Year', columns='DS_SECTORID', aggfunc='count') #Check number of firms per sector per year - compare with JSE All share Index - Price as proxy for all observations
ObsVariableYear.to_excel(ExportDir + 'ObsVariableYear.xlsx', sheet_name='Input')
ObsSectorYearCount.to_excel(ExportDir + 'ObsSectorYearCount.xlsx', sheet_name='Input') #work in excel to create %percentages

# Create Dataset1 which has returns, market premium and riskfree rate starting from BBBEEStartYear
BBBEEStartYear = 2004
Dataset1 = Dataset0.loc[(Dataset0['Year']>=(BBBEEStartYear-1))]
PriceLogReturnMatrix = Dataset1.pivot_table('PriceLogReturnCum', index='Year', columns='FirmID')
tmpYear = pd.DataFrame(PriceLogReturnMatrix.index.get_level_values(0))
tmpYear['YearIndex'] = tmpYear.index.get_level_values(0)

# FF BP and SIZE top 30% Dummy
FactorDecile = pd.DataFrame(['SIZE','BP'])
FactorDecile.columns = ['Factors']
for ii in range(FactorDecile.shape[0]):    
    inpFactor = FactorDecile['Factors'][ii]
    FactorDF = pd.DataFrame(columns=['FirmID','Year',str(inpFactor+'_Decile')])
    for jj in range(tmpYear.shape[0]):
        inpYear = tmpYear['Year'][jj]
        tmpFactorDF = Dataset1.loc[(Dataset1['Year']==inpYear),['FirmID','Year',inpFactor]]
        tmpFactorDF[str(inpFactor+'_Decile')] = pd.qcut(tmpFactorDF[inpFactor],10,labels=False)
        tmpFactorDF = tmpFactorDF.drop_duplicates()
        tmpFactorDF = tmpFactorDF[['FirmID','Year',str(inpFactor+'_Decile')]]
        FactorDF = pd.concat([FactorDF, tmpFactorDF])    
        del inpYear, tmpFactorDF
    Dataset1 = pd.merge(Dataset1,FactorDF,on=['FirmID','Year'],how='left')
    del inpFactor, FactorDF

del ii,jj    
BPMatrix = Dataset1.pivot_table('BP_Decile', index='Year', columns='FirmID')
BPTopDummy = ([BPMatrix <=2]) #FF use top and bottom 30% see http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/det_port_form_sz.html
BPTopDummy = BPTopDummy[0]
BPBottomDummy = ([BPMatrix >=7])
BPBottomDummy = BPBottomDummy[0]
SIZEMatrix = Dataset1.pivot_table('SIZE_Decile', index='Year', columns='FirmID')
SIZETopDummy = ([SIZEMatrix <=2])
SIZETopDummy = SIZETopDummy[0]
SIZEBottomDummy = ([SIZEMatrix >=7])
SIZEBottomDummy = SIZEBottomDummy[0]

MarketReturn = tmpYear[['Year','YearIndex']]
MarketPremium = tmpYear[['Year','YearIndex']]
BPIndex = tmpYear[['Year','YearIndex']]
SIZEIndex = tmpYear[['Year','YearIndex']]
RiskFreeProxy = 'SA10YR'
RiskFreeReturn = Dataset1[['Year',RiskFreeProxy]]
RiskFreeReturn = RiskFreeReturn.drop_duplicates()
RiskFreeReturn[RiskFreeProxy] = RiskFreeReturn[RiskFreeProxy]/100
RiskFreeReturn = RiskFreeReturn.reset_index(drop=True)
RiskFreeReturn['RFR_Compound'] = (1+RiskFreeReturn[RiskFreeProxy]).cumprod() -1
PriceLogReturn = Dataset1[['Year','FirmID']]

InpReturnHorizonYears = pd.DataFrame([1,2,3,4,5])
InpReturnHorizonYears.columns = ['ReturnHorizonYears']

for ii in range(InpReturnHorizonYears.shape[0]): #Create returns, BPIndex, SIZEIndex, RiskFreeReturn and MarketReturn
    tmpReturnHorizonYears = InpReturnHorizonYears['ReturnHorizonYears'][ii]
    tmpDF = PriceLogReturnMatrix
    tmpDF = tmpDF.fillna(0) #replace nan with 0 to be able to calculate yearly return
    if tmpReturnHorizonYears > 1:
        tmpDF1 = pd.DataFrame(np.array((1+ tmpDF.shift(-tmpReturnHorizonYears))/(1+tmpDF)-1)) #calculate forward looking return, to calculate for BBBEE score in year 0 what was return from year 0 to year tmpReturnHorizonYears    
        RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)] = pd.DataFrame(np.array(((1+ RiskFreeReturn['RFR_Compound'].shift(-(tmpReturnHorizonYears-1)))/(1+RiskFreeReturn['RFR_Compound']))-1)) #calculate compound riskfree return over tmpReturnHorizonYears
        RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)] = pd.DataFrame(np.array((1+RiskFreeReturn[RiskFreeProxy])*(1+RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)])-1)) #add return on the year to have the riskfreereturn for tmpReturnHorizonYears
    else:    
        tmpDF1 = pd.DataFrame(np.array((1+tmpDF)/(1+ tmpDF.shift(tmpReturnHorizonYears))-1))
        RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)] = pd.DataFrame(np.array((1+RiskFreeReturn['RFR_Compound'])/(1+ RiskFreeReturn['RFR_Compound'].shift(tmpReturnHorizonYears))-1))      
    tmpDF2 = Functions.StackDFDS_simple(tmpDF1,str('PriceLogReturn_YR'+str(tmpReturnHorizonYears)))
    tmpDF2.columns = [str('PriceLogReturn_YR'+str(tmpReturnHorizonYears)),'FirmID','YearIndex']
    tmpDF2 = pd.merge(tmpDF2,tmpYear,on='YearIndex',how='left')
    tmpDF2 = tmpDF2[[str('PriceLogReturn_YR'+str(tmpReturnHorizonYears)),'FirmID','Year']]
    PriceLogReturn = pd.merge(PriceLogReturn,tmpDF2,on=['Year','FirmID'],how='left')    
    MarketReturn['MarketReturn_YR'+str(tmpReturnHorizonYears)] = np.nanmean(np.array(tmpDF1), axis=1) #calculate market return over time horizon    
    MarketPremium['MarketPremium_YR'+str(tmpReturnHorizonYears)] = MarketReturn['MarketReturn_YR'+str(tmpReturnHorizonYears)] - RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)]
    tmpBPTop = pd.DataFrame(np.array(BPTopDummy)*np.array(tmpDF1))
    tmpBPTop = pd.DataFrame(np.nanmean(tmpBPTop, axis=1))
    tmpBPBottom = pd.DataFrame(np.array(BPBottomDummy)*np.array(tmpDF1))
    tmpBPBottom = pd.DataFrame(np.nanmean(tmpBPBottom, axis=1))    
    BPIndex['BPIndex_YR'+str(tmpReturnHorizonYears)] = tmpBPTop - tmpBPBottom
    tmpSIZETop = pd.DataFrame(np.array(SIZETopDummy)*np.array(tmpDF1))
    tmpSIZETop = pd.DataFrame(np.nanmean(tmpSIZETop, axis=1))
    tmpSIZEBottom = pd.DataFrame(np.array(SIZEBottomDummy)*np.array(tmpDF1))
    tmpSIZEBottom = pd.DataFrame(np.nanmean(tmpSIZEBottom, axis=1))
    SIZEIndex['SIZEIndex_YR'+str(tmpReturnHorizonYears)] = tmpSIZETop - tmpSIZEBottom
    del tmpDF, tmpDF1, tmpDF2, tmpBPTop, tmpBPBottom, tmpSIZETop, tmpSIZEBottom

del ii
#Adjust BBBEE Rank data
YearBBBEECount= Dataset1[['BBBEE_Rank','Year']].groupby('Year').count()
YearBBBEECount.loc[(YearBBBEECount['BBBEE_Rank']==0)] = float('nan')
tmpPlot = YearBBBEECount
tmpPlot = tmpPlot.reset_index()
Functions.SimpleLineChart('BBBEE_Rank','Year',tmpPlot,ExportDir,'Number of BBBEE ranked observations','') #plot observation of BBBEE through time
del tmpPlot
MinBBBEECount = round(YearBBBEECount['BBBEE_Rank'].min(),-1) # find lowest number of observations BBBEE Rank rounded on nearest 10
tmpCleanRank = Dataset1.groupby('Year')['BBBEE_Rank'].rank(ascending=True) #rerank BBBEE based on year 
Dataset1['BBBEE_Rank_Clean'] = tmpCleanRank
Dataset1.loc[(Dataset1['BBBEE_Rank_Clean'] > MinBBBEECount),'BBBEE_Rank_Clean'] = float('nan')
tmpCleanRank2 = Dataset1.groupby('Year')['BBBEE_Rank_Clean'].rank(ascending=False) #make BBBEE Rank higher the better - to improve interpretability in regressions
Dataset1['BBBEE_Rank_Clean'] = tmpCleanRank2
del tmpCleanRank, tmpCleanRank2

#Add industry dummy
tmpSECTORID = StagingFirm[['DS_SECTORNAME','DS_SECTORID']].drop_duplicates()
tmpSECTORID = tmpSECTORID.reset_index(drop=True)
SectorDummy = Dataset1[['Year','FirmID','DS_SECTORID']]
for ii in range(tmpSECTORID.shape[0]): #create a dummy variable name for all DS_SECTORS
    tmpColumnName = str('DSDUMMY_' + str(tmpSECTORID['DS_SECTORNAME'][ii]))
    SectorDummy[str(tmpColumnName)] = np.zeros(shape=(SectorDummy.shape[0],1))
    SectorDummy.loc[(SectorDummy['DS_SECTORID'] == tmpSECTORID['DS_SECTORID'][ii]),str(tmpColumnName)] = 1

#compare with JSE
tmpSectorDataset1 = Dataset1[['FirmID','Year','DS_SECTORID','BBBEE_Rank_Clean']] # Dataset1
tmpSectorDataset1 = tmpSectorDataset1.dropna()
tmpSectorDatasetPivot = tmpSectorDataset1.pivot_table('FirmID', index='Year', columns='DS_SECTORID', aggfunc='count')
tmpSectorDatasetPivot.reset_index(drop=True)
tmpSectorDatasetPivot = tmpSectorDatasetPivot.fillna(0)
tmpYearSector = tmpSectorDataset1[['BBBEE_Rank_Clean','Year']].groupby('Year').count()
tmpYearSector = pd.DataFrame(np.matlib.repmat(np.array(tmpYearSector), 1, tmpSectorDatasetPivot.shape[1]))
tmpSector1 = np.array(tmpSectorDatasetPivot) / np.array(tmpYearSector)

StagingJSE = pd.merge(StagingJSE,tmpSECTORID,on='DS_SECTORNAME',how='left') #calc JSE 
tmpJSE = StagingJSE[['DS_SECTORNAME','DS_SECTORID']].groupby('DS_SECTORID').count()
tmpJSETot = StagingJSE['DS_SECTORNAME'].count()
tmpJSE = tmpJSE/tmpJSETot
tmpJSE = np.transpose(tmpJSE)
tmpJSE2 = np.matlib.repmat(np.array(tmpJSE), tmpSectorDatasetPivot.shape[0],1)

tmpSectorYears = tmpSectorDatasetPivot.reset_index() #Calc diff
tmpSectorYears = tmpSectorYears[['Year']]
tmpJSEDataset1Diff = tmpSector1 - tmpJSE2 

tmpJSEDataset1Diff = pd.DataFrame(tmpJSEDataset1Diff) #prep for export
tmpJSEDataset1Diff['Year'] = tmpSectorYears
tmpJSEDataset1Diff = tmpJSEDataset1Diff.set_index('Year', drop=True)
tmpColumns = tmpSECTORID.set_index('DS_SECTORID',drop=True)
tmpColumns=tmpColumns.sort_index()
tmpColumns = list(tmpColumns['DS_SECTORNAME'])
tmpJSEDataset1Diff.columns = tmpColumns
tmpJSEDataset1Diff.to_excel(ExportDir + "RelativeDifference_JSE_Dataset.xlsx",sheet_name='Input')

del tmpSECTORID,ii
SectorDummy = SectorDummy.drop(['DS_SECTORID'], axis=1)

#Finalize Variables
Dataset1 = Dataset1.loc[(Dataset1['Year']>=BBBEEStartYear)] #finalize Dataset1
Dataset1 = Dataset1.loc[(np.isnan(np.array(Dataset1['BBBEE_Rank_Clean']))==False)]#remove all rows with no BBBEE value
SectorDummy = SectorDummy.loc[(SectorDummy['Year']>=BBBEEStartYear)] #finalize SectorDummy
BPIndex = BPIndex.loc[(BPIndex['Year']>=BBBEEStartYear)] #finalize BPIndex
SIZEIndex = SIZEIndex.loc[(SIZEIndex['Year']>=BBBEEStartYear)] #finalize BPIndex
MarketPremium = MarketPremium.loc[(MarketPremium['Year']>=BBBEEStartYear)] #finalize MarketPremium
MarketReturn = MarketReturn.loc[(MarketReturn['Year']>=BBBEEStartYear)] #finalize MarketReturn
RiskFreeReturn = RiskFreeReturn.loc[(RiskFreeReturn['Year']>=BBBEEStartYear)] #finalize RiskFreeReturn
PriceLogReturn = PriceLogReturn.loc[(PriceLogReturn['Year']>=BBBEEStartYear)] 

#Run Descriptives over different time horizons - describe
Describe1 = pd.DataFrame(Dataset1['FirmID'].describe())
Describe1 = Functions.DescribeExNaN(Dataset1,'BP',Describe1)
Describe1 = Functions.DescribeExNaN(Dataset1,'SIZE',Describe1)
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(BPIndex,'BPIndex_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1)) #describes over all the InputYears for this factor
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(SIZEIndex,'SIZEIndex_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(MarketPremium,'MarketPremium_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(MarketReturn,'MarketReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(RiskFreeReturn,'RiskFreeReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(PriceLogReturn,'PriceLogReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1.to_excel(ExportDir + 'Descriptives.xlsx', sheet_name='Input')

#Run Descriptives over different time horizons - describe, scatterplots, correlation matrix
InpReturnHorizonYears = pd.DataFrame([1,2,3,4,5])
InputYears = InpReturnHorizonYears
OutputSet = Dataset1[['Year','FirmID','BP','SIZE','BBBEE_Rank_Clean']]
RegressionOutputSimple = {}
RegressionOutputNormal = {}

for ii in range(InputYears.shape[0]): #see https://lectures.quantecon.org/py/ols.html
    tmpOutput = pd.merge(Dataset1[['Year','FirmID','BP','SIZE','BBBEE_Rank_Clean']],BPIndex[['Year',str('BPIndex_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput['BBBEE_Rank'] = tmpOutput['BBBEE_Rank_Clean']
    tmpOutput = tmpOutput.drop('BBBEE_Rank_Clean', axis=1)
    tmpOutput = pd.merge(tmpOutput,SIZEIndex[['Year',str('SIZEIndex_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,MarketPremium[['Year',str('MarketPremium_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,RiskFreeReturn[['Year',str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,PriceLogReturn[['Year','FirmID',str('PriceLogReturn_YR'+ str(InputYears[0][ii]))]],on=['Year','FirmID'],how='left')
    tmpOutput = pd.merge(tmpOutput,SectorDummy,on=['Year','FirmID'],how='left')
    
    #correlation matrix
    tmpCorrelationMatrix = pd.DataFrame(tmpOutput.corr())
    tmpCorrelationMatrix.to_excel(ExportDir + 'CorrelationMatrix_YR' + str(InputYears[0][ii])+ '.xlsx', sheet_name='Input')
   
    #create scatterplots 
    tmpXColumns = ['BP','SIZE','BBBEE_Rank',str('BPIndex_YR'+ str(InputYears[0][ii])),str('SIZEIndex_YR'+ str(InputYears[0][ii])),str('MarketPremium_YR'+ str(InputYears[0][ii])),str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))]
    tmpYColumn = str('PriceLogReturn_YR'+ str(InputYears[0][ii]))    
    Functions.PriceLogScatterplots(tmpXColumns,tmpYColumn,tmpOutput,ExportDir)    
    del tmpXColumns,tmpYColumn
                   
    #Define Y and X and standardize X column names
    tmpY = tmpOutput[[str('PriceLogReturn_YR'+ str(InputYears[0][ii]))]]    
    tmpX1 = tmpOutput.drop(['Year','FirmID',str('BPIndex_YR'+ str(InputYears[0][ii])),str('SIZEIndex_YR'+ str(InputYears[0][ii])),str('PriceLogReturn_YR'+ str(InputYears[0][ii]))], axis=1)
    tmpX1 = Functions.OLSStandardizeXCol(tmpX1)    
    tmpX2 = tmpOutput.drop(['Year','FirmID','BP','SIZE',str('PriceLogReturn_YR'+ str(InputYears[0][ii]))], axis=1)
    tmpX2 = Functions.OLSStandardizeXCol(tmpX2)    
    
    #Run Regression over different time horizons - simple bp, size, bpIndex, sizeIndex and     
    tmpOLSSimpleFF = sm.OLS(tmpY,tmpX1, missing='drop')
    tmpOLSSimpleFFResults = tmpOLSSimpleFF.fit()
    tmpKey = str('SimpleFF' + str(InputYears[0][ii]))   # https://stackoverflow.com/questions/5036700/how-can-you-dynamically-create-variables-via-a-while-loop 
    tmpValue = tmpOLSSimpleFFResults    
    RegressionOutputSimple[tmpKey] = tmpValue    
    del tmpKey, tmpValue
        
    tmpOLSNormalFF = sm.OLS(tmpY,tmpX2, missing='drop')
    tmpOLSNormalFFResults = tmpOLSNormalFF.fit()  
    tmpKey1 = str('NormalFF' + str(InputYears[0][ii]))   # https://stackoverflow.com/questions/5036700/how-can-you-dynamically-create-variables-via-a-while-loop 
    tmpValue1 = tmpOLSNormalFFResults    
    RegressionOutputNormal[tmpKey1] = tmpValue1  
    del tmpKey1, tmpValue1
            
    del tmpOutput,tmpCorrelationMatrix,tmpX1,tmpX2,tmpY,tmpOLSSimpleFF,tmpOLSSimpleFFResults,tmpOLSNormalFF,tmpOLSNormalFFResults
 
del ii
Functions.Regression5YearOutput(RegressionOutputSimple,ExportDir,'OLS_Summary_' ) # Output Regression results Simple
Functions.Regression5YearOutput(RegressionOutputNormal,ExportDir,'OLS_Summary_' ) # Output Regression results Simple

#Adjust for outliers - Dataset2 - RiskFreeReturn is not adjusted 
Dataset2 = Dataset1 #Already contains clean BBBEE
tmpYear2 = tmpYear.loc[(tmpYear['Year']>=BBBEEStartYear)] #ensure to only take years for which BBBBEE data is available
tmpYear2 = tmpYear2.reset_index(drop=True)
InpReturnHorizonYears = pd.DataFrame([1,2,3,4,5])
InpReturnHorizonYears.columns = ['ReturnHorizonYears']

#Adjust BP, SIZE
Dataset2 = Dataset2.reset_index(drop=True)
tmpColumns = ['BP','SIZE']
for ii in range(len(tmpColumns)): 
    inpColumn = str(tmpColumns[ii])
    tmpDF1 = Functions.CapOutliers(Dataset2,inpColumn)
    Dataset2 = Dataset2.drop([inpColumn], axis=1)
    Dataset2 = pd.merge(Dataset2,tmpDF1,on=['Year','FirmID'],how='left')
del tmpColumns, ii

#Adjust PriceLogReturns for outliers - Create PriceLogReturn2
PriceLogReturn2 = PriceLogReturn[['Year','FirmID']]
tmpColumns = pd.DataFrame(PriceLogReturn.columns)
tmpColumns = tmpColumns.loc[(tmpColumns[0] != 'Year')]
tmpColumns = tmpColumns.loc[(tmpColumns[0] != 'FirmID')]
tmpColumns = list(tmpColumns[0])
for ii in range(len(tmpColumns)): 
    inpColumn = str(tmpColumns[ii])
    tmpDF1 = Functions.CapOutliers(PriceLogReturn,inpColumn)
    PriceLogReturn2 = pd.merge(PriceLogReturn2,tmpDF1,on=['Year','FirmID'],how='left')
del tmpColumns, ii

#Rerun Deciles
Dataset2 = Dataset2.drop(['SIZE_Decile','BP_Decile'], axis=1)

#Rerun factor returns
FactorDecile2 = pd.DataFrame(['SIZE','BP'])
FactorDecile2.columns = ['Factors']
for ii in range(FactorDecile2.shape[0]):    
    inpFactor2 = FactorDecile2['Factors'][ii]
    FactorDF2 = pd.DataFrame(columns=['FirmID','Year',str(inpFactor2+'_Decile')])
    for jj in range(tmpYear2.shape[0]):
        inpYear = tmpYear2['Year'][jj]
        tmpFactorDF2 = Dataset2.loc[(Dataset2['Year']==inpYear),['FirmID','Year',inpFactor2]]
        tmpFactorDF2 = tmpFactorDF2.drop_duplicates()
        tmpFactorDF2[str(inpFactor2+'_Decile')] = pd.qcut(tmpFactorDF2[inpFactor2],10,labels=False)
        tmpFactorDF2 = tmpFactorDF2[['FirmID','Year',str(inpFactor2+'_Decile')]]
        FactorDF2 = pd.concat([FactorDF2, tmpFactorDF2])    
        del inpYear, tmpFactorDF2
    Dataset2 = pd.merge(Dataset2,FactorDF2,on=['FirmID','Year'],how='left')
    del inpFactor2, FactorDF2

del ii,jj 
BPMatrix2 = Dataset2.pivot_table('BP_Decile', index='Year', columns='FirmID')
BPTopDummy2 = ([BPMatrix2 <=2])
BPTopDummy2 = BPTopDummy2[0]
BPBottomDummy2 = ([BPMatrix2 >=7])
BPBottomDummy2 = BPBottomDummy2[0]
SIZEMatrix2 = Dataset2.pivot_table('SIZE_Decile', index='Year', columns='FirmID')
SIZETopDummy2 = ([SIZEMatrix2 <=2])
SIZETopDummy2 = SIZETopDummy2[0]
SIZEBottomDummy2 = ([SIZEMatrix2 >=7])
SIZEBottomDummy2 = SIZEBottomDummy2[0]

#Rerun MarketReturn, MarketPremium, BPIndex, SIZEIndex
MarketReturn2 = tmpYear2[['Year','YearIndex']]
MarketPremium2 = tmpYear2[['Year','YearIndex']]
BPIndex2 = tmpYear2[['Year','YearIndex']]
SIZEIndex2 = tmpYear2[['Year','YearIndex']]
BPTopDummy2 = BPTopDummy[BPTopDummy.index>=BBBEEStartYear] #source: https://stackoverflow.com/questions/24088795/python-pandas-select-index-where-index-is-larger-than-x
BPBottomDummy2 = BPBottomDummy[BPBottomDummy.index>=BBBEEStartYear] 

for ii in range(InpReturnHorizonYears.shape[0]):
    tmpReturnHorizonYears = InpReturnHorizonYears['ReturnHorizonYears'][ii]
    tmpPriceLogReturnColumn = str('PriceLogReturn_YR' + str(tmpReturnHorizonYears))
    tmpDF = PriceLogReturn2.pivot_table(tmpPriceLogReturnColumn, index='Year', columns='FirmID')
    MarketReturn2['MarketReturn_YR'+str(tmpReturnHorizonYears)] = np.nanmean(np.array(tmpDF), axis=1) #calculate market return over time horizon    
    MarketPremium2['MarketPremium_YR'+str(tmpReturnHorizonYears)] = MarketReturn2['MarketReturn_YR'+str(tmpReturnHorizonYears)] - RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)]
    tmpBPTop = pd.DataFrame(np.array(BPTopDummy2)*np.array(tmpDF))
    tmpBPTop = pd.DataFrame(np.nanmean(tmpBPTop, axis=1))
    tmpBPBottom = pd.DataFrame(np.array(BPBottomDummy2)*np.array(tmpDF))
    tmpBPBottom = pd.DataFrame(np.nanmean(tmpBPBottom, axis=1))    
    BPIndex2['BPIndex_YR'+str(tmpReturnHorizonYears)] = tmpBPTop - tmpBPBottom
    tmpSIZETop = pd.DataFrame(np.array(SIZETopDummy2)*np.array(tmpDF))
    tmpSIZETop = pd.DataFrame(np.nanmean(tmpSIZETop, axis=1))
    tmpSIZEBottom = pd.DataFrame(np.array(SIZEBottomDummy2)*np.array(tmpDF))
    tmpSIZEBottom = pd.DataFrame(np.nanmean(tmpSIZEBottom, axis=1))
    SIZEIndex2['SIZEIndex_YR'+str(tmpReturnHorizonYears)] = tmpSIZETop - tmpSIZEBottom
    del tmpDF, tmpBPTop, tmpBPBottom, tmpSIZETop, tmpSIZEBottom

#Run Descriptives over different time horizons - describe
Describe2 = pd.DataFrame(Dataset2['FirmID'].describe())
Describe2 = Functions.DescribeExNaN(Dataset2,'BP',Describe2)
Describe2 = Functions.DescribeExNaN(Dataset2,'SIZE',Describe2)
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(BPIndex2,'BPIndex_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2)) #describes over all the InputYears for this factor
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(SIZEIndex2,'SIZEIndex_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2))
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(MarketPremium2,'MarketPremium_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2))
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(MarketReturn2,'MarketReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2))
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(RiskFreeReturn,'RiskFreeReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2))
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(PriceLogReturn2,'PriceLogReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2))
Describe2.to_excel(ExportDir + 'Descriptives_OutlierAdjusted.xlsx', sheet_name='Input')

#Run Descriptives over different time horizons - describe, scatterplots, correlation matrix
InpReturnHorizonYears = pd.DataFrame([1,2,3,4,5])
InputYears = InpReturnHorizonYears
OutputSet2 = Dataset2[['Year','FirmID','BP','SIZE','BBBEE_Rank_Clean']]
RegressionOutputSimple2 = {}
RegressionOutputNormal2 = {}

for ii in range(InputYears.shape[0]): #see https://lectures.quantecon.org/py/ols.html
    tmpOutput = pd.merge(Dataset2[['Year','FirmID','BP','SIZE','BBBEE_Rank_Clean']],BPIndex2[['Year',str('BPIndex_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput['BBBEE_Rank'] = tmpOutput['BBBEE_Rank_Clean']
    tmpOutput = tmpOutput.drop('BBBEE_Rank_Clean', axis=1)
    tmpOutput = pd.merge(tmpOutput,SIZEIndex2[['Year',str('SIZEIndex_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,MarketPremium2[['Year',str('MarketPremium_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,RiskFreeReturn[['Year',str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,PriceLogReturn2[['Year','FirmID',str('PriceLogReturn_YR'+ str(InputYears[0][ii]))]],on=['Year','FirmID'],how='left')
    tmpOutput = pd.merge(tmpOutput,SectorDummy,on=['Year','FirmID'],how='left')
    
    #correlation matrix
    tmpCorrelationMatrix = pd.DataFrame(tmpOutput.corr())
    tmpCorrelationMatrix.to_excel(ExportDir + 'CorrelationMatrix_OutlierAdjusted_YR' + str(InputYears[0][ii])+ '.xlsx', sheet_name='Input')
   
    #create scatterplots 
    tmpXColumns = ['BP','SIZE','BBBEE_Rank',str('BPIndex_YR'+ str(InputYears[0][ii])),str('SIZEIndex_YR'+ str(InputYears[0][ii])),str('MarketPremium_YR'+ str(InputYears[0][ii])),str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))]
    tmpYColumn = str('PriceLogReturn_YR'+ str(InputYears[0][ii]))    
    Functions.PriceLogScatterplots(tmpXColumns,tmpYColumn,tmpOutput,ExportDir)    
    del tmpXColumns,tmpYColumn
                   
    #Define Y and X and standardize X column names
    tmpY = tmpOutput[[str('PriceLogReturn_YR'+ str(InputYears[0][ii]))]]    
    tmpX1 = tmpOutput.drop(['Year','FirmID',str('BPIndex_YR'+ str(InputYears[0][ii])),str('SIZEIndex_YR'+ str(InputYears[0][ii])),str('PriceLogReturn_YR'+ str(InputYears[0][ii]))], axis=1)
    tmpX1 = Functions.OLSStandardizeXCol(tmpX1)    
    tmpX2 = tmpOutput.drop(['Year','FirmID','BP','SIZE',str('PriceLogReturn_YR'+ str(InputYears[0][ii]))], axis=1)
    tmpX2 = Functions.OLSStandardizeXCol(tmpX2)    
    
    #Run Regression over different time horizons - simple bp, size, bpIndex, sizeIndex and     
    tmpOLSSimpleFF = sm.OLS(tmpY,tmpX1, missing='drop')
    tmpOLSSimpleFFResults = tmpOLSSimpleFF.fit()
    tmpKey = str('SimpleFF' + str(InputYears[0][ii]))   # https://stackoverflow.com/questions/5036700/how-can-you-dynamically-create-variables-via-a-while-loop 
    tmpValue = tmpOLSSimpleFFResults    
    RegressionOutputSimple2[tmpKey] = tmpValue    
    del tmpKey, tmpValue
        
    tmpOLSNormalFF = sm.OLS(tmpY,tmpX2, missing='drop')
    tmpOLSNormalFFResults = tmpOLSNormalFF.fit()  
    tmpKey1 = str('NormalFF' + str(InputYears[0][ii]))   # https://stackoverflow.com/questions/5036700/how-can-you-dynamically-create-variables-via-a-while-loop 
    tmpValue1 = tmpOLSNormalFFResults    
    RegressionOutputNormal2[tmpKey1] = tmpValue1  
    del tmpKey1, tmpValue1
            
    del tmpOutput,tmpCorrelationMatrix,tmpX1,tmpX2,tmpY,tmpOLSSimpleFF,tmpOLSSimpleFFResults,tmpOLSNormalFF,tmpOLSNormalFFResults
    
Functions.Regression5YearOutput(RegressionOutputSimple2,ExportDir,'OLS_Summary_OutlierAdjusted' ) # Output Regression results Simple
Functions.Regression5YearOutput(RegressionOutputNormal2,ExportDir,'OLS_Summary_OutlierAdjusted' ) # Output Regression results Simple
"""