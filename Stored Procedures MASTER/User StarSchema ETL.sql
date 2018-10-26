use pacifica; 

DROP PROCEDURE IF EXISTS User_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `User_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN


/********** BEGIN OF STORED PROCEDURE ***********/

/*
Source is cc_UserRecord
Target is User_Dim


Using an Effective_Datetime from the Change Capture Etl process to check for multiple "batch" loads.  This date is loaded into the Dim table when records are inserted or changed.
It serves as the Batch Run identifier and is compared to ensure duplicates are not inserted if the load was called twice in the same <time period>.  The time period can be 
adjusted.   Once the date changes it would allow the load to perform again.  This is an unlikely corner case, but could be improved if needed.

Approach for Slowly Changing Dimension is to Insert new records, and for changed records, 1. update the existing active 
record by changing it to INActive, and 2, inserting (UpSert) a new Active Record.   We never Update data (other than the SCD fields) on a record.

So the process steps are 
1. Identify cc_UserRecord records that are Updates.  Log them.
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
	Set ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_UserRecord ); 
    
    


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


-- Get all of the existing UserSource_Record_ID values from User_Dim with the Extract Datetime.  We treat Effective_Datetime as a logical Batch ID.
Drop Table If Exists Existing_Dim_IDs;
Create temporary table Existing_Dim_IDs (ID bigint, Effective_Datetime datetime);
Insert Into Existing_Dim_IDs
Select distinct User_ID, Effective_Datetime from User_dim where Active_Flag = 1; 






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
cc.ID as User_ID,
cc.modified as Last_Modified_Datetime,
Case When cc.deleted <> dim.deleted_datetime Then 1 Else 0 End as Deleted_Date_Changed,
Case When cc.AccountID <> dim.Account_ID Then 1 Else 0 End as Account_ID_Changed,
Case When cc.permissions <> dim.permissions Then 1 Else 0 End as User_Role_Changed,
Case When cc.status <> dim.status Then 1 Else 0 End as User_Status_Changed,
Case When cc.name <> dim.full_name Then 1 Else 0 End as Full_Name_Changed,
Case When cc.firstname <> dim.first_name Then 1 Else 0 End as First_Name_Changed,
Case When cc.lastname <> dim.last_name Then 1 Else 0 End as Last_Name_Changed,
Case When cc.email <> dim.email Then 1 Else 0 End Email_Changed,
Case When cc.Department <> dim.department Then 1 Else 0 End Department_Changed,
Case When cc.Timezonetype <> dim.timezonetype Then 1 Else 0 End as Timezone_Changed,
Case When cc.passwordexpiration <> dim.Password_Expiration_Datetime Then 1 Else 0 End as Password_Expiration_Changed

From cc_UserRecord cc

Inner Join User_dim dim
On cc.id = dim.User_ID

Where dim.Active_Flag = 1     --  Only perform this change compare to the latest, active record in the Dim table.

;



-- Pivot the results with insert statements to load the TChange_Log_Ref table.  
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
"User" ,
User_ID, 
"Deleted_Date_Changed"
From TChange_Log 
Where Deleted_Date_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"User" ,
User_ID, 
"Account ID Changed"
From TChange_Log 
Where Account_ID_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"User" ,
User_ID, 
"User Role Changed"
From TChange_Log 
Where User_Role_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"User" ,
User_ID, 
"Active Status Changed"
From TChange_Log 
Where User_Status_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"User" ,
User_ID, 
"Full_Name_Changed"
From TChange_Log 
Where Full_Name_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"User" ,
User_ID, 
"First Name Changed"
From TChange_Log 
Where First_Name_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"User" ,
User_ID, 
"Last Name Changed"
From TChange_Log 
Where Last_Name_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"User" ,
User_ID, 
"Email Changed"
From TChange_Log 
Where Email_Changed = 1 ; 

 
Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"User" ,
User_ID, 
"Department Changed"
From TChange_Log 
Where Department_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"User" ,
User_ID, 
"Timezone Changed"
From TChange_Log 
Where Timezone_Changed = 1 ; 

 
 Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"User" ,
User_ID, 
"Password Expiration Changed"
From TChange_Log 
Where Password_Expiration_Changed = 1 ; 








/********************** PERFORM SLOWLY CHANGING DIMENSION UPDATES TO DIM RECORDS ******************/

/*
The process steps are 
1. Identify cc_UserRecord records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.
*/


-- These are your distinct IDs that are to be Up-Serted.   select distinct Source_Record_ID from TChange_Log_Ref


-- Expire old records that have changed.
UPDATE	User_Dim ad
Inner Join TChange_Log_Ref cl 		-- these are your change capture records that are updates.
On cl.Source_Record_ID = ad.User_ID
Set ad.Active_Flag = 0,
ad.Expiration_Datetime = ETL_Datetime 	-- Use the ETL_Datetime consistently for datetime references within this ETL process.
Where ad.Active_Flag = 1 ; 			-- For an UserRecord, only expire the curretn Active record.








/****************************  PERFORM INSERTS AND UP-SERTS  **********************************/

/*
This step inserts NEW dimension ID records and inserts new Active records for existing IDs ( "Up-Sert" rather than an "Update" )

*/



-- Perform Insert for any new UserRecords.
INSERT INTO User_dim
(
	Effective_Datetime,
    Expiration_Datetime,
    Active_Flag,
    Deleted_Flag,
    Deleted_Datetime,
    User_ID,
    Account_ID,
    User_Role,
    First_Added_Datetime,
    Last_Modified_Datetime,
    User_Status,
    Full_Name,
    First_Name,
    Last_Name,
    Email,
    Department,
    TimeZone,
    Password_Expiration_Flag,
    Password_Expiration_Datetime,
    
    -- Admin
    TimezoneType,
    Status,
    Permissions,
    ETL_Run_Datetime
)



SELECT	Distinct		-- This is the Mapping between cc_UserRecord and User_dim, with transformations ... mostly lookups against mtype references.

ETL_Datetime as Effective_Datetime,
null as Expiration_Datetime,
1 as Active_Flag,

Case When Deleted is not null Then 1 Else 0 End as Deleted_Flag,
Deleted as Deleted_Datetime,

cc.ID as User_ID,
cc.accountid as Account_ID,

m2.name as User_Role,
cc.Created as First_Added_Datetime,
cc.Modified as Last_Modified_Datetime,
m3.name as User_Status,

cc.name as Full_Name,
cc.firstname as First_Name,
cc.lastname as Last_Name,
cc.email as Email,
cc.department as   Department,
m4.name as Time_Zone,
    
Case When passwordexpiration is not null Then 1 Else 0 End as Password_Expiration_Flag,
cc.passwordexpiration as Password_Expiration,

--  Admin Fields    
cc.TimezoneType,
cc.status,
cc.Permissions,
ETL_Datetime
    

From cc_UserRecord cc

left outer join mtype_2 m2
on cc.permissions = m2.id

left outer join mtype_3 m3
on cc.status = m3.id

left outer join mtype_4 m4
on cc.timezonetype = m4.id


left outer join Existing_Dim_IDs ED  -- Only insert records for UserRecord IDs that do not previously exist in User_Dim.
on cc.ID = ED.ID

left outer join TChange_Log_Ref clf
on cc.ID = clf.Source_Record_ID


-- You want to perform Inserts for all new cc_UserRecord records, all changed existing records, but not existing records in cc_UserRecord that did not change.
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



-- Update all of the ID values in the TChange_Log table for the inserts that were just performed. 
UPDATE	Change_Log r
Inner Join User_Dim d 				-- these are your change capture records that are updates.
On r.table_name = "User"
And r.source_record_id = d.User_id
And d.active_flag = 1
Set r.Record_ID = d.ID    				-- This is the auto_increment record id.
;




/****************  CLEAN UP ANY TEMPORARY TABLES ***************/

Drop Table if exists mtype_2;
Drop Table if exists mtype_3;
Drop Table if exists mtype_4;
Drop Table If Exists Existing_Dim_IDs;
Drop Table If Exists TChange_Log ; 
Drop Table If Exists TChange_Log_Ref_2 ;



COMMIT;

END$$
DELIMITER ;



-- End of script

