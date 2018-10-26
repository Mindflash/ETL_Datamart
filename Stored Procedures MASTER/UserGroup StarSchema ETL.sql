use pacifica; 

DROP PROCEDURE IF EXISTS UserGroup_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `UserGroup_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN


/********** BEGIN OF STORED PROCEDURE ***********/

/*
Source is cc_UserGroup
Target is UserGroup_Dim

Using an Effective_Datetime from the Change Capture Etl process to check for multiple "batch" loads.  This date is loaded into the Dim table when records are inserted or changed.
It serves as the Batch Run identifier and is compared to ensure duplicates are not inserted if the load was called twice in the same <time period>.  The time period can be 
adjusted.   Once the date changes it would allow the load to perform again.  This is an unlikely corner case, but could be improved if needed.

Approach for Slowly Changing Dimension is to Insert new records, and for changed records, 1. update the existing active 
record by changing it to INActive, and 2, inserting (UpSert) a new Active Record.   We never Update data (other than the SCD fields) on a record.

So the process steps are 
1. Identify cc_UserGroup records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.


*/




-- DECLARATIONS
    
	-- Use Effective_Datetime to ensure a single value is used for all records.
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
	Set  ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_UserGroup ); 

    


-- Check to see if this logical batch has already been loaded, and abort.
--  cast(cc.Effective_Datetime as date)  <>  ifnull(cast(Aid.Effective_Datetime as date),cast("1900-01-01" as date))    




/*************** Prepare some working tables ******************/

-- Get all of the existing User_ID values from UserGroup_Dim with the Extract Datetime.  We treat Effective_Datetime as a logical Batch ID.
Drop Table If Exists Existing_Dim_IDs;
Create temporary table Existing_Dim_IDs (ID bigint, Effective_Datetime datetime);
Insert Into Existing_Dim_IDs
Select distinct User_Group_ID, Effective_Datetime from User_Group_Dim where Active_Flag = 1; 






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
cc.usergroupuserID as UserGroupUser_ID,
cc.modified as Last_Modified_Datetime,
Case When cc.userid <> dim.user_id Then 1 Else 0 End as User_ID_Changed,
Case When cc.accountid <> dim.account_ID Then 1 Else 0 End as Account_ID_Changed,
Case When cc.deleted <> dim.deleted_date Then 1 Else 0 End as Deleted_Date_Changed,
Case When cc.name <> dim.group_name Then 1 Else 0 End as Group_Name_Changed

From cc_UserGroup cc

Inner Join User_Group_dim dim
On cc.usergroupuserid = dim.User_Group_ID

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
"UserGroup" ,
UserGroupUser_ID, 
"User ID Changed"
From TChange_Log 
Where User_ID_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
 ETL_Datetime, 
Last_Modified_Datetime,
"UserGroup" ,
UserGroupUser_ID, 
"Account_ID Changed"
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
"UserGroup" ,
UserGroupUser_ID,  
"Deleted Date Changed"
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
"UserGroup" ,
UserGroupUser_ID, 
"Group Name Changed"
From TChange_Log 
Where Group_Name_Changed = 1 ; 






/********************** PERFORM SLOWLY CHANGING DIMENSION UPDATES TO DIM RECORDS ******************/

/*
The process steps are 
1. Identify cc_UserGroup records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.
*/


-- These are your distinct IDs that are to be Up-Serted.   select distinct Source_Record_ID from TChange_Log_Ref


-- Expire old records that have changed.
UPDATE	User_Group_Dim d
Inner Join TChange_Log_Ref cl 		-- these are your change capture records that are updates.
On cl.Source_Record_ID = d.User_Group_ID
Set d.Active_Flag = 0,
d.Expiration_Datetime =  ETL_Datetime 	-- Use the  ETL_Datetime consistently for datetime references within this ETL process.
Where d.Active_Flag = 1 ; 			-- For an UserGroup, only expire the curretn Active record.







/****************************  PERFORM INSERTS AND UP-SERTS  **********************************/

/*
This step inserts NEW dimension ID records and inserts new Active records for existing IDs ( "Up-Sert" rather than an "Update" )

*/


-- Perform Insert for any new UserGroups.
INSERT INTO User_Group_Dim 
(Effective_Datetime,
Expiration_Datetime,
Active_Flag,
Deleted_Flag,
Deleted_Date,
First_Added_Datetime ,
Last_Modified_Datetime ,
User_Group_ID,
User_ID,
Account_ID,
Group_Name,

-- Admin Fields
ETL_Run_Datetime
)

SELECT	Distinct		-- This is the Mapping between cc_UserGroup and UserGroup_dim, with transformations ... mostly lookups against mtype references.

 ETL_Datetime as Effective_Datetime,    
null as Expiration_Datetime,
1 as Active_Flag,
Case When Deleted is not null Then 1 Else 0 End as Deleted_Flag,
Deleted as Deleted_Date,
cc.created as First_Added_Date,
cc.modified as Last_Modified_Date,
cc.usergroupuserID,
cc.userid,
cc.AccountID,
cc.name,
ETL_Datetime   

From cc_UserGroup cc

left outer join Existing_Dim_IDs ED  -- Only insert records for UserGroup IDs that do not previously exist in UserGroup_Dim.
on cc.UserGroupUserID = ED.ID

left outer join TChange_Log_Ref clf
on cc.UserGroupUserID = clf.Source_Record_ID


-- You want to perform Inserts for all new cc_UserGroup records, all changed existing records, but not existing records in cc_UserGroup that did not change.
Where 

-- New Records.
ED.ID is Null

OR

-- Changed Records.
clf.Source_Record_ID is not null

;
 







/****************  UPDATE The Permanent Change_Log that is AVailable for Reporting ***************/
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


-- Update all of the ID values in the TChange_Log table for the UpSerts that were just performed. 
UPDATE	Change_Log r
Inner Join User_Group_Dim d 				-- these are your change capture records that are updates.
On r.table_name = "UserGroup"
And r.source_record_id = d.User_Group_id
And d.active_flag = 1
Set r.Record_ID = d.ID    				-- This is the auto_increment record id.
;



-- Perform a de-dup on the table based on  ETL_Datetime, Table_Name, Source_Record_ID, Data_Change_Message.   Do this once at the end of an entire Load Cycle.






/****************  CLEAN UP ANY TEMPORARY TABLES ***************/
Drop Table If Exists Existing_Dim_IDs;
Drop Table If Exists TChange_Log ; 
Drop Table If Exists TChange_Log_Ref_2 ;







COMMIT;

END$$
DELIMITER ;



-- End of script

