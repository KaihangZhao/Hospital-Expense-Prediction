# Hospital-Expense-Prediction

## Intro

This is a project to use PySpark(Spark SQL, Spark ML) and Mongodb to build distributed machine learning prediction on New York Hospital Expenses in patient level. There are three targests we consider in total: Length of Stay, Cost, Charges and Copayment Proportion. Our team members are  *Wei He*(whe13@usfca.edu), *Kaihang Zhao*(kzhao24@usfca.edu), *Jih-Chin Chen*(jchen217@usfca.edu), *Tong Wang*(twang77@usfca.edu) and *Yangzhou Tang*(ytang47@usfca.edu).

<img src="picture/pipeline.png" width="450" align="right"> In this project, we created **MongoDB Atlas** and sharded the data into three clusters to improve data management. Then we created a data pipeline to access **20GB+** of raw data from **AWS S3** and transferred the cleaned data to MongoDB Atlas. After performing data preprocessing, we loaded data from MongoDB Atlas and applied machine learning algorithms in a distributed system on **Databricks** using **Spark ML**.

## Raw Data Description

Our data contains New York Hospital Discharges data in the patient level from 2011 to 2013. 
Reference: https://health.data.ny.gov/Health/Hospital-Inpatient-Discharges-SPARCS-De-Identified/u4ud-w55t
| Variable | DataType | Detail |
|--------|--------|--------|
| `Health_Service_Area` | String | A description of the Health Service Area (HSA) in which the hospital is located. Blank for abortion records. Capital/Adirondack, Central NY, Finger Lakes, Hudson Valley, Long Island, New York City, Southern Tier, Western NY.|
| `Hospital_County` | String | A description of the county in which the hospital is located. Blank for abortion records. |
| `Operating_Certificate_Number` | String | The facility Operating Certificate Number as assigned by NYS Department of Health. Blank for abortion records. |
| `Facility_ID` | Numeric | Permanent Facility Identifier. Blank for abortion records. |
| `Facility_Name` | String | The name of the facility where services were performed based on the Permanent Facility Identifier (PFI), as maintained by the NYSDOH Division of Health Facility Planning. For abortion records ‘Abortion Record – Facility Name Redacted’ appears. |
| `Age_Group` | String | Age in years at time of discharge. Grouped into the following age groups: 0 to 17, 18 to 29, 30 to 49, 50 to 69, and 70 or Older.|
| `Zip_Code` | String | Type is Char. Length is 3. The first three digits of the patient's zip code. Blank for: - population size less than 20,000 - abortion records, or - cell size less than 10 on population classification strata. “OOS” are Out of State zip codes. |
| `Gender` | String | Patient gender: (M) Male, (F) Female, (U) Unknown |
| `Race` | String | Patient race. Black/African American, Multi, Other Race, Unknown, White. Other Race includes Native Americans and Asian/Pacific Islander. |
| `Ethnicity` | String | Patient ethnicity. The ethnicity of the patient: Spanish/Hispanic Origin, Not of Spanish/Hispanic Origin, Multi, Unknown. |
| `Length_of_Stay` | String | The total number of patient days at an acute level and/or other than acute care level (excluding leave of absence days) (Discharge Date - Admission Date) + 1. Length of Stay greater than or equal to 120 days has been aggregated to 120+ days. |
| `Type_of_Admission` | String | A description of the manner in which the patient was admitted to the health care facility: Elective, Emergency, Newborn, Not Available, Trauma, Urgent. |
| `Patient_Disposition` | String  | The patient's destination or status upon discharge. |
| `Discharge_Year` | String | The year (CCYY) of discharge. |
| `CCS_Diagnosis_Code` | Numeric| AHRQ Clinical Classification Software (CCS) Diagnosis Category Code. More information on the CCS system may be found at the direct link: http://www.hcup-us.ahrq.gov/toolssoftware/ccs/ccs.jsp|
| `CCS_Diagnosis_Description` | String | AHRQ Clinical Classification Software (CCS) Diagnosis Category Description. More information on the CCS system may be found at the direct link: http://www.hcup-us.ahrq.gov/toolssoftware/ccs/ccs.jsp |
| `CCS_Procedure_Code` | Numeric | AHRQ Clinical Classification Software (CCS) ICD-9 Procedure Category Code. More information on the CCS system may be found at the direct link: http://www.hcup-us.ahrq.gov/toolssoftware/ccs/ccs.jsp |
| `CCS_Procedure_Description` | String | AHRQ Clinical Classification Software (CCS) ICD-9 Procedure Category Description. More information on the CCS system may be found at the direct link: http://www.hcup-us.ahrq.gov/toolssoftware/ccs/ccs.jsp |
| `review` | Text | the text of the written review |
| `unix_timestamp_created` | Timestamp | date the review was created (unix timestamp) |
| `unix_timestamp_updated` | Timestamp | date the review was last updated (unix timestamp) |
| `unix_timestamp_updated` | Timestamp | date the review was last updated (unix timestamp) |
| `num_reviews` | Numeric | the number of reviews this user has written |
| `review` | Text | the text of the written review |
| `unix_timestamp_created` | Timestamp | date the review was created (unix timestamp) |
| `unix_timestamp_updated` | Timestamp | date the review was last updated (unix timestamp) |
| `unix_timestamp_updated` | Timestamp | date the review was last updated (unix timestamp) |




   
                     StructField('CCS_Procedure_Code', IntegerType(), True),\
                     StructField('CCS_Procedure_Description', StringType(), True),\
                     StructField('APR_DRG_Code', IntegerType(), True),\
                     StructField('APR_DRG_Description', StringType(), True),\
                     StructField('APR_MDC_Code', IntegerType(), True),\
                     StructField('APR_MDC_Description', StringType(), True),\
                     StructField('APR_Severity_of_Illness_Code', IntegerType(), True),\
                     StructField('APR_Severity_of_Illness_Description', StringType(), True),\
                     StructField('APR_Risk_of_Mortality', StringType(), True),\
                     StructField('APR_Medical_Surgical_Description', StringType(), True),\
                     StructField('Payment_Typology_1', StringType(), True),\
                     StructField('Payment_Typology_2', StringType(), True),\
                     StructField('Payment_Typology_3', StringType(), True),\
                     StructField('Birth_Weight', StringType(), True),\
                     StructField('Abortion_Edit_Indicator', StringType(), True),\
                     StructField('Emergency_Department_Indicator', StringType(), True),\
                     StructField('Total_Charges', StringType(), True),\
                     StructField('Total_Costs', StringType(), True)]





## Data Preprocessing


## ML Predictions
We built Random Forest models and Gradient Boosting models to predict three difference target, including length of stay, total costs and total charges. Using R_squared and Feature Importances as metrics, we evaluated and assessed each model and extracted intelligence from our models.

### 1. Length of Stay
### 2. Total Costs
### 3. Total Charges

## Conclusion & Lesson Learned
