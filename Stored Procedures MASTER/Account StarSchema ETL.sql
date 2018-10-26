use pacifica; 

DROP PROCEDURE IF EXISTS Account_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Account_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN


/********** BEGIN OF STORED PROCEDURE ***********/

/*
Source is cc_account
Target is Account_Dim

Using an Effective_Datetime from the Change Capture Etl process to check for multiple "batch" loads.  This date is loaded into the Dim table when records are inserted or changed.
It serves as the Batch Run identifier and is compared to ensure duplicates are not inserted if the load was called twice in the same <time period>.  The time period can be 
adjusted.   Once the date changes it would allow the load to perform again.  This is an unlikely corner case, but could be improved if needed.

Approach for Slowly Changing Dimension is to Insert new records, and for changed records, 1. update the existing active 
record by changing it to INActive, and 2, inserting (UpSert) a new Active Record.   We never Update data (other than the SCD fields) on a record.

So the process steps are 
1. Identify cc_account records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.

*/









-- DECLARATIONS
    
	-- Set Effective_Datetime to ensure a single value is used for all records.
	Declare ETL_Datetime datetime;
        
	-- SQLEXCEPTION DECLARATION
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN	
			-- ERROR
            SET SP_Result = 1 ;
            SET SP_MESSAGE = 'An error has occurred, operation rollbacked and the stored procedure was terminated';
			ROLLBACK;
		END;
		

-- START TRANSACTION    
	START TRANSACTION;


-- SET VARIABLES
    Set SP_Result = 0;

	-- This date serves as a "Batch Datetime" and links the cc data to the dim data.
	Set ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_account ); 
    


-- Check to see if this logical batch has already been loaded, and abort.
--  cast(cc.Effective_Datetime as date)  <>  ifnull(cast(Aid.Effective_Datetime as date),cast("1900-01-01" as date))    






/*************** Prepare some working tables ******************/

-- MySQL does not support multiple references to the same table, so will need multiple temp tables for mtype
Drop Table if exists mtype_2;
Create Temporary Table mtype_2
(  ID int(11) NOT NULL,
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL 
) ;
Insert into mtype_2 Select ID, name from mtype_ref;

Drop Table if exists mtype_3;
Create Temporary Table mtype_3
(  ID int(11) NOT NULL,
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL 
) ;
Insert into mtype_3 Select ID, name from mtype_ref;

Drop Table if exists mtype_4;
Create Temporary Table mtype_4
(  ID int(11) NOT NULL,
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL 
) ;
Insert into mtype_4 Select ID, name from mtype_ref;

Drop Table if exists mtype_5;
Create Temporary Table mtype_5
(  ID int(11) NOT NULL,
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL 
) ;
Insert into mtype_5 Select ID, name from mtype_ref;

Drop Table if exists tier_2;
Create Temporary Table tier_2
(  ID int(11) NOT NULL,
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL 
) ;
Insert into tier_2 Select ID, name from tier_ref;


-- Get all of the existing Account_ID values from Account_Dim with the Extract Datetime.  We treat Effective_Datetime as a logical Batch ID.
Drop Table If Exists Existing_Dim_IDs;
Create temporary table Existing_Dim_IDs (ID bigint, Effective_Datetime datetime);
Insert Into Existing_Dim_IDs
Select distinct Account_ID, Effective_Datetime from account_dim where Active_Flag = 1; 






/****************** UPDATE CHANGE LOG WITH ALL RECORD UPDATES ********************/


-- This approach explicitly checks each field that defines a record change for the reporting application.   It's safe because it depends on the actual data and not the 
--    application code that updates the MODIFIED value.   It adds rules that need to be maintained, but enables explicit messaging for each data that changes.

-- This Temporary TChange_Log table is specific to the Dimension.  It is pivoted below to conform to the standard TChange_Log table.
-- This step captures all (multiple) column changes per record.  
Drop Table If Exists TChange_Log ; 
Create Temporary Table TChange_Log 
Select Distinct
ETL_Datetime,
cc.ETL_Run_Datetime as Extract_Datetime,
cc.ID as Account_ID,
cc.modified as Last_Modified_Datetime,
Case When cc.deleted <> dim.deleted_Datetime Then 1 Else 0 End as Deleted_Datetime_Changed,
Case When cc.name <> dim.account_name Then 1 Else 0 End as Account_Name_Changed,     -- Using SalesForce name lookup, so will not match in those cases where a match exists.
Case When cc.description <> dim.account_description Then 1 Else 0 End as Account_Description_Changed,
Case When cc.salesforceenabled <> dim.SalesforceAppEnabled_Datetime Then 1 Else 0 End as SalesForce_Enabled_Datetime_Changed,
Case When cc.owneremail <> dim.Owner_Email Then 1 Else 0 End as Owner_Email_Changed,
Case When cc.disabled  <> dim.Disabled_Datetime Then 1 Else 0  End as Disabled_Datetime_Changed,
Case When cc.pricingtype <> dim.pricingtype Then 1  Else 0  End as Pricing_Type_Changed,
Case When cc.trialexpiration  <> dim.Trial_Expiration_Datetime  Then 1  Else 0  End as Trial_Expiration_Datetime_Changed,
Case When cc.status <> dim.status then 1  End as Account_Status_Changed ,
Case When cc.statusmodified <> dim.Account_Status_Modified_Datetime Then 1 Else 0 End as Account_Status_Modified_Datetime_Changed,
Case When cc.cancellationdate <> dim.Cancellation_Datetime Then 1  Else 0  End as Cancellation_Datetime_Changed, 
Case When cc.pricingmaxtrainees <> dim.Pricing_Max_Trainees Then 1 Else 0 End as Pricing_Max_Trainees_Changed,
Case When cc.billingemail <> dim.Billing_Email then 1 Else 0 End as Billing_Email_Changed ,
Case When cc.billingalternateemail <> dim.Billing_Alternate_Email Then 1 Else 0 End as Billing_Alt_Email_Changed , 
Case When cc.apienabled <> dim.Api_Enabled_Datetime Then 1  Else 0  End as API_Enabled_Changed,
Case When cc.inactivitywarningsent <> dim.Inactivity_Warning_Sent_Datetime Then 1 Else 0 End as Inactivity_Warning_Sent_Changed,
Case When cc.promocode <> dim.Promo_Code Then 1 Else 0 End as Promo_Code_Changed,
Case When cc.inactivityfinalwarningsent <> dim.Inactivity_Final_Warning_Sent_Datetime Then 1 Else 0 End as Inactivity_Final_Warning_Changed,
Case When cc.phonenumber <> dim.Phone_Number Then 1 Else 0 End as Phone_Number_Changed,
Case When cc.usenewdeletionrules <> Dim.Use_New_Deletion_Rules_Flag Then 1 Else 0 End as Use_New_Deletion_Rules_Changed ,
Case When cc.billingintervaltype <> dim.billingintervaltype Then 1 Else 0 End as Billing_Interval_Type_Changed,
Case When cc.tierID <> dim.TierID Then 1 Else 0 End as Tier_Changed,
Case When cc.trialtierID <> dim.trialtierID Then 1 Else 0 End as TrialTier_Changed,
Case When cc.loginmode <> dim.loginmode Then 1 Else 0 End as Login_Mode_Changed,
Case When cc.Graceperioddays <> dim.Grace_Period_Days Then 1 Else 0 End as Grace_Period_Days_Changed,
Case When cc.SAMLEnabled <> dim.SAMLEnabled_Datetime Then 1 Else 0 End as SAMLEnabled_Changed,
Case When cc.previoustrials <> dim.PreviousTrials_Number Then 1 Else 0 End as Previous_Trials_Number_Changed,
Case When cc.custombrandingenabled <> dim.Custom_Branding_Enabled_Flag Then 1 Else 0 End as Custom_Branding_Enabled_Changed,
Case When cc.hidemindflashbranding <> dim.Hide_Mindflash_Branding_Flag Then 1 Else 0 End as Hide_Mindflash_Branding_Changed,
Case When cc.shopifyshopname <> dim.Shopify_Shop_Name Then 1 Else 0 End as Shopify_Shop_Name_Changed,
Case When cc.optin <> Dim.GDPC_Opt_In_Flag Then 1 Else 0 End as GDPC_Opt_In_Changed,
Case When cc.maintenancemode <> dim.Maintenance_Mode_Flag Then 1 Else 0 End as Maintenance_Mode_Changed

From cc_account cc

Inner Join account_dim dim
On cc.id = dim.Account_ID

Where dim.Active_Flag = 1     --  Only perform this change compare to the latest, active record in the Dim table.

;




-- Pivot the results with insert statements to load the TTChange_Log_Ref table.  
-- May seems cumbersome, but it enables specific messaging for each column, and is still a relational set operation (not a cursor loop).

-- Lets' put these in a temp table so that we can use it in the update process without querying a larger dataset.  At the end we will load these to the main TChange_Log.
	
Drop Table If Exists TChange_Log_Ref ;     
Create Temporary Table TChange_Log_Ref (
Effective_Datetime datetime,
Last_Modified_Datetime datetime,
Table_Name varchar(25), 
Source_Record_ID bigint,
Data_Change_Message varchar(255) ) ;
    
    

Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Deleted Date Changed"
From TChange_Log 
Where Deleted_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Account Name Changed"
From TChange_Log 
Where  Account_Name_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Account Description Changed"
From TChange_Log 
Where  Account_Description_Changed= 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"SalesForce Enabled Date Changed"
From TChange_Log 
Where SalesForce_Enabled_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Owner Email Changed"
From TChange_Log 
Where Owner_Email_Changed = 1 ; 

Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Disabled Date Changed"
From TChange_Log 
Where Disabled_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Pricing Type Changed"
From TChange_Log 
Where Pricing_Type_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Trial Expiration Date Changed"
From TChange_Log 
Where Trial_Expiration_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Account Status Changed"
From TChange_Log 
Where Account_Status_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Account Status Modified Date Changed"
From TChange_Log 
Where Account_Status_Modified_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Cancellation Date Changed"
From TChange_Log 
Where Cancellation_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Cancellation Date Changed"
From TChange_Log 
Where Cancellation_Datetime_Changed = 1 ; 

 
Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Pricing Max Trainees Changed"
From TChange_Log 
Where Pricing_Max_Trainees_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Billing Email Changed"
From TChange_Log 
Where Billing_Email_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Billing Alt Email Changed"
From TChange_Log 
Where Billing_Alt_Email_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"API Enabled Changed"
From TChange_Log 
Where API_Enabled_Changed = 1 ; 

Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Inactivity Warning Sent Changed"
From TChange_Log 
Where Inactivity_Warning_Sent_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Promo Code Changed"
From TChange_Log 
Where Promo_Code_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Inactivity Final Warning Changed"
From TChange_Log 
Where Inactivity_Final_Warning_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Phone Number Changed"
From TChange_Log 
Where Phone_Number_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Use New Deletion Rules Changed"
From TChange_Log 
Where Use_New_Deletion_Rules_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Billing Interval Type Changed"
From TChange_Log 
Where Billing_Interval_Type_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Tier Changed"
From TChange_Log 
Where Tier_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Trial TierChanged"
From TChange_Log 
Where TrialTier_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Login Mode Changed"
From TChange_Log 
Where Login_Mode_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Grace Period Days Changed"
From TChange_Log 
Where Grace_Period_Days_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"SAMLEnabled Changed"
From TChange_Log 
Where SAMLEnabled_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Previous Trials Number Changed"
From TChange_Log 
Where Previous_Trials_Number_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Custom Branding Enabled Changed"
From TChange_Log 
Where Custom_Branding_Enabled_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Hide Mindflash Branding Changed"
From TChange_Log 
Where Hide_Mindflash_Branding_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Shopify Shop Name Changed"
From TChange_Log 
Where Shopify_Shop_Name_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"GDPC Opt In Changed"
From TChange_Log 
Where GDPC_Opt_In_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID , 
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Account" ,
Account_ID, 
"Maintenance Mode Changed"
From TChange_Log 
Where Maintenance_Mode_Changed = 1 ;






/********************** PERFORM SLOWLY CHANGING DIMENSION UPDATES TO DIM RECORDS ******************/

/*
The process steps are 
1. Identify cc_account records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.
*/


-- These are your distinct IDs that are to be Up-Serted.   select distinct Source_Record_ID from TChange_Log_Ref


-- Expire old records that have changed.
UPDATE	Account_Dim ad
Inner Join TChange_Log_Ref cl 		-- these are your change capture records that are updates.
On cl.Source_Record_ID = ad.Account_ID
Set ad.Active_Flag = 0,
ad.Expiration_Datetime = ETL_Datetime 	-- Use the ETL_Datetime consistently for datetime references within this ETL process.
Where ad.Active_Flag = 1 ; 			-- For an account, only expire the current Active record.







/****************************  PERFORM INSERTS AND UP-SERTS  **********************************/

/*
This step inserts NEW dimension ID records and inserts new Active records for existing IDs ( "Up-Sert" rather than an "Update" )

*/



-- Perform Insert for any new accounts.
INSERT INTO account_dim
(Effective_Datetime,
Expiration_Datetime,
Active_Flag,
Deleted_Flag,
Deleted_Datetime,
Account_ID,
Account_Name,
Account_Description,
First_Added_Datetime,
Last_Modified_Datetime,
Salesforce_Enabled_Flag,
SalesforceAppEnabled_Datetime,
Owner_Email,
Disabled_Flag,
Disabled_Datetime,
Pricing_Type,
Trial_Expiration_Datetime,
Account_Status,
Account_Status_Modified_Datetime,
Cancellation_Datetime,
Pricing_Max_Trainees,
Billing_Email,
Billing_Alternate_Email,
API_Enabled_Flag,
API_Enabled_Datetime,
Inactivity_Warning_Sent_Datetime,
Promo_Code,
Inactivity_Final_Warning_Sent_Datetime,
Phone_Number,
Use_New_Deletion_Rules_Flag,
Billing_Interval_Type,
Account_Tier,
Max_Team_Members_Number,
Advanced_Tier_Trial_Expiration_Datetime,
Trial_Tier,
Login_Mode,
Grace_Period_Days,
SAMLEnabled_Flag,
SAMLEnabled_Datetime,
PreviousTrials_Number,
Custom_Branding_Enabled_Flag,
Hide_Mindflash_Branding_Flag,
Shopify_Shop_Name,
GDPC_Opt_In_Flag,
Maintenance_Mode_Flag,
-- Admin Fields
PricingType,
Status, 
BillingIntervalType,
LoginMode, 
TierID,
TrialTierID,
ETL_Run_Datetime
)

SELECT	Distinct		-- This is the Mapping between cc_account and account_dim, with transformations ... mostly lookups against mtype references.

ETL_Datetime as Effective_Datetime,
null as Expiration_Datetime,
1 as Active_Flag,

Case When Deleted is not null Then 1 Else 0 End as Deleted_Flag,
Deleted as Deleted_Date,

cc.ID as Account_ID,
Case when sf.Name is not null then sf.name else cc.name end as Account_Name,
cc.Description as Account_Description,
cc.Created as First_Added_Date,
cc.Modified as Last_Modified_Date,
Case when salesforceenabled is not null then 1 else 0 end as Salesforce_Enabled_Flag,
salesforceenabled as SalesforceAppEnabled_Datetime,
owneremail as Owner_Email,
Case when disabled is not null then 1 else 0 end as Disabled_Flag,
disabled as Disabled_Datetime,
m2.name as Pricing_Type,
trialexpiration as Trial_Expiration_Datetime,
m3.name as Account_Status,
cc.statusmodified as Account_Status_Modified_Datetime,
cancellationdate as Cancellation_Date,
pricingmaxtrainees as Pricing_Max_Trainees,
billingemail as Billing_Email,
billingalternateemail as Billing_Alternate_Email,

Case When apienabled is not null then 1 else 0 end as API_Enabled_Flag,
apienabled as API_Enabled_Datetime,
inactivitywarningsent as Inactivity_Warning_Sent_Datetime,
promocode as Promo_Code,
inactivityfinalwarningsent as Inactivity_Final_Warning_Sent_Datetime,
phonenumber as Phone_Number,

usenewdeletionrules as Use_New_Deletion_Rules_Flag,
m4.name as Billing_Interval_Type,
t.name as Account_Tier,
maxteammembers as Max_Team_Members_Number,
advancedtiertrialexpiration as Advanced_Tier_Trial_Expiration_Datetime,
t2.name as Trial_Tier,
m5.name as Login_Mode,

graceperioddays as Grace_Period_Days,
Case When SAMLEnabled is not null then 1 else 0 end as SAMLEnabled_Flag,
SAMLEnabled as SAMLEnabled_Datetime,

previoustrials as PreviousTrials_Number,

custombrandingenabled as Custom_Branding_Enabled_Flag,
hidemindflashbranding as Hide_Mindflash_Branding_Flag,
shopifyshopname as Shopify_Shop_Name,
Optin as GDPC_Opt_In_Flag,
maintenancemode as Maintenance_Mode_Flag,

-- Admin Fields
PricingType,
Status, 
BillingIntervalType,
LoginMode,
TierID,
TrialTierID  ,
ETL_Datetime

From cc_account cc

left outer join mtype_2 m2
on cc.pricingtype = m2.id

left outer join mtype_3 m3
on cc.status = m3.id

left outer join mtype_4 m4
on cc.billingintervaltype = m4.id

left outer join tier_ref t
on cc.tierid = t.id

left outer join tier_2 t2
on cc.trialtierid = t2.id

left outer join mtype_5 m5
on cc.loginmode = m5.id

left outer join accountsalesforce sf
on cc.id = sf.accountid

left outer join Existing_Dim_IDs ED  -- Only insert records for Account IDs that do not previously exist in Account_Dim.
on cc.ID = ED.ID

left outer join TChange_Log_Ref clf
on cc.ID = clf.Source_Record_ID


-- You want to perform Inserts for all new cc_account records, all changed existing records, but not existing records in cc_account that did not change.
Where 

-- New Records.
ED.ID is Null

OR

-- Changed Records.
clf.Source_Record_ID is not null

;





/****************  UPDATE The Permanent Change_Log that is Available for Reporting ***************/
-- Need to ensure this only occurs once per logical batch.   

Insert Into Change_Log
(
Effective_Datetime ,
Last_Modified_Datetime,
Table_Name , 
Source_Record_ID , 
Data_Change_Message
)
Select 
Effective_Datetime ,
Last_Modified_Datetime,
Table_Name , 
Source_Record_ID , 
Data_Change_Message
from TChange_Log_Ref ; 




-- Update all of the ID values in the Change_Log table for the UpSerts that were just performed. 
UPDATE	Change_Log r
Inner Join Account_Dim d 				
On r.table_name = "Account"
And r.source_record_id = d.account_id
And d.active_flag = 1
Set r.Record_ID = d.ID    				-- This is the auto_increment record id.
;




/****************  CLEAN UP ANY TEMPORARY TABLES ***************/

Drop Table if exists mtype_2;
Drop Table if exists mtype_3;
Drop Table if exists mtype_4;
Drop Table if exists mtype_5;
Drop Table if exists tier_2;
Drop Table If Exists Existing_Dim_IDs;
Drop Table If Exists TChange_Log ; 
Drop Table If Exists TChange_Log_Ref_2 ;





COMMIT;

END$$
DELIMITER ;



-- End of script

