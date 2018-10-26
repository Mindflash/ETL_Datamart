use pacifica; 

DROP PROCEDURE IF EXISTS Module_Participation_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Module_Participation_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN

 



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
    Set sp_result = 0;

	-- This date serves as a "Batch Datetime" and links the cc data to the fact data.
	Set ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_CmoduleState ); 
    



/*************** Prepare some working tables ******************/

-- Get all of the existing CModuleState_ID values from Module_Participation_Fact with the Extract Datetime.  We treat Effective_Datetime as a logical Batch ID.
Drop Table If Exists Existing_Dim_IDs;
Create temporary table Existing_Dim_IDs (ID bigint, Effective_Datetime datetime);
Insert Into Existing_Dim_IDs
Select distinct CModuleState_ID, Effective_Datetime from Module_Participation_Fact where Active_Flag = 1; 



/****************** UPDATE CHANGE LOG WITH ALL RECORD UPDATES ********************/

-- This approach explicitly checks each field that defines a record change for the reporting application.   It's safe because it depends on the actual data and not the 
--    application code that updates the MODIFIED value.   It adds rules that need to be maintained, but enables explicit messaging for each data that changes.

-- This Temporary TChange_Log table is specific to the Dimension.  It is pivoted below to conform to the standard Data_Change_Ref table.
-- This step captures all (multiple) column changes per record.  
Drop Table If Exists TChange_Log ; 

Create Temporary Table TChange_Log 
Select Distinct
ETL_Datetime,    
cc.ETL_Run_Datetime as Extract_Datetime,
cc.ID as CModuleState_ID,
cc.modified as Last_Modified_Datetime,
Case When ifnull(cc.deleted,'1900-01-01') <> fact.deleted_datetime Then 1 Else 0 End as Deleted_Date_Changed,
Case When cc.accountid <> fact.account_id Then 1 Else 0 End as Account_ID_Changed,
Case When cc.status <> fact.status_id Then 1 Else 0 End as Status_Changed,

-- Datetime fields are defaulted to 1900-01-01 when NULL. So we must compare the cc NULL value to this value in the fact table.
Case When ifnull(cc.invited,'1900-01-01') <> fact.invited_datetime  Then 1 Else 0 End as Invited_Datetime_Changed,
Case When ifnull(cc.uninvited,'1900-01-01') <> fact.uninvited_datetime  Then 1 Else 0 End as UnInvited_Datetime_Changed,
Case When ifnull(cc.invitationemailsent,'1900-01-01') <> fact.invitation_email_sent_datetime Then 1 Else 0 End as Invitation_Email_Sent_Changed,
Case When ifnull(cc.reminderemailsent,'1900-01-01') <> fact.reminder_email_sent_Datetime Then 1 Else 0 End as Reminder_Email_Sent_Changed,
Case When ifnull(cc.started,'1900-01-01') <> fact.Module_started_datetime Then 1 Else 0 End as Module_Started_Changed,
Case When ifnull(cc.completed,'1900-01-01') <> fact.Module_completion_datetime Then 1 Else 0 End as Module_Completed_Changed,
Case When ifnull(cc.completionemailsent,'1900-01-01') <> fact.completion_email_sent_datetime Then 1 Else 0 End as Completion_Email_Sent_Changed,
Case When ifnull(cc.pokereminderemailsent,'1900-01-01') <> fact.poke_reminder_email_sent_datetime Then 1 Else 0 End as Poke_Reminder_Email_Sent_Changed,
Case When ifnull(cc.lastactivity,'1900-01-01') <> fact.Module_last_activity_datetime Then 1 Else 0 End as Module_Last_Activity_Changed,
Case When ifnull(cc.completiondeadline,'1900-01-01') <> fact.trainee_completion_deadline_datetime Then 1 Else 0 End as Completion_Deadline_Changed,
Case When ifnull(cc.invitationemailrespondeddate,'1900-01-01') <> fact.invitation_email_responded_datetime Then 1 Else 0 End as Invite_Email_Responded_Changed,


-- These are non-date change tests
Case When cc.finalgrade <> fact.Module_final_grade_percent  Then 1 Else 0 End as Module_Final_Grade_Changed,
Case When cc.required <> fact.Module_required_flag Then 1 Else 0 End as Module_Required_Flag, 
Case When cc.duration <> fact.Module_elapsed_duration_seconds Then 1 Else 0 End as Duration_Changed,
Case When cc.progress <> fact.in_progress_complete_percent  Then 1 Else 0 End as In_Progress_Changed,
Case When ifnull(cc.invitedbyuserid,-1) <> fact.invited_by_user_id  Then 1 Else 0 End as Invited_By_User_Changed,
Case When ifnull(cc.uninvitedbyuserid,-1) <> fact.Uninvited_by_user_id  Then 1 Else 0 End as UnInvited_By_User_Changed,
Case When cc.gradestatus <> fact.Module_grade_status_id Then 1 Else 0 End as Module_Grade_Status_Changed,
Case When cc.product <> fact.product_id Then 1 Else 0 End as Product_Changed,
Case When cc.hadquiz <> fact.took_quiz_flag Then 1 Else 0 End as Took_Quiz_Changed

 
From cc_CModuleState cc

Inner Join Module_Participation_Fact fact
On cc.id = fact.CmoduleState_ID

Where fact.Active_Flag = 1     --  Only perform this change compare to the latest, active record in the fact table.

;






-- Pivot the results with insert statements to load the TChange_Log_Ref table.  
-- May seems cumbersome, but it enables specific messaging for each column, and is still a relational set operation (not a cursor loop).

-- Lets' put these in a temp table so that we can use it in the update process without querying a larger dataset.  At the end we will load these to the main Data_Change_Ref.
	
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
"CModuleState" ,
CModuleState_ID, 
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
"CModuleState" ,
CModuleState_ID, 
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
"CModuleState" ,
CModuleState_ID, 
"Status_Changed"
From TChange_Log 
Where Status_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Invited_Datetime_Changed"
From TChange_Log 
Where Invited_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"UnInvited_Datetime_Changed"
From TChange_Log 
Where UnInvited_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Invitation_Email_Sent_Changed"
From TChange_Log 
Where Invitation_Email_Sent_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Reminder_Email_Sent_Changed"
From TChange_Log 
Where Reminder_Email_Sent_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Module_Started_Changed"
From TChange_Log 
Where Module_Started_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Module_Completed_Changed"
From TChange_Log 
Where Module_Completed_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Module_Final_Grade_Changed"
From TChange_Log 
Where Module_Final_Grade_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Completion_Email_Sent_Changed"
From TChange_Log 
Where Completion_Email_Sent_Changed = 1 ; 



Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Poke_Reminder_Email_Sent_Changed"
From TChange_Log 
Where Poke_Reminder_Email_Sent_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Module_Grade_Status_Changed"
From TChange_Log 
Where Module_Grade_Status_Changed = 1 ; 



Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Module_Last_Activity_Changed"
From TChange_Log 
Where Module_Last_Activity_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Completion_Deadline_Changed"
From TChange_Log 
Where Completion_Deadline_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Duration_Changed"
From TChange_Log 
Where Duration_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"In_Progress_Changed"
From TChange_Log 
Where In_Progress_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Invited_By_User_Changed"
From TChange_Log 
Where Invited_By_User_Changed = 1 ; 



Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Invite_Email_Responded_Changed"
From TChange_Log 
Where Invite_Email_Responded_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Module_Required_Flag"
From TChange_Log 
Where Module_Required_Flag = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Invited_By_User_Changed"
From TChange_Log 
Where Invited_By_User_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Product_Changed"
From TChange_Log 
Where Product_Changed = 1 ; 



Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CModuleState" ,
CModuleState_ID, 
"Took_Quiz_Changed"
From TChange_Log 
Where Took_Quiz_Changed = 1 ; 




/********************** PERFORM SLOWLY CHANGING DIMENSION UPDATES TO fact RECORDS ******************/

/*
The process steps are 
1. Identify cc_CourseState records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.
*/


-- These are your distinct IDs that are to be Up-Serted.   select distinct Source_Record_ID from TChange_Log_Ref


-- Expire old records that have changed.
UPDATE	Module_Participation_Fact ad
Inner Join TChange_Log_Ref cl 		-- these are your change capture records that are updates.
On cl.Source_Record_ID = ad.CModuleState_ID
Set ad.Active_Flag = 0,
ad.Expiration_Datetime = ETL_Datetime 	-- Use the ETL_Datetime consistently for datetime references within this ETL process.
Where ad.Active_Flag = 1 ; 			-- For an Course, only expire the curretn Active record.







/****************************  PERFORM INSERTS AND UP-SERTS  **********************************/

/*
This step inserts NEW dimension ID records and inserts new Active records for existing IDs ( "Up-Sert" rather than an "Update" )

*/



-- Perform Insert for any new Courses.
INSERT INTO Module_Participation_Fact
( Effective_Datetime,
  Expiration_Datetime,
  Active_Flag,
  Deleted_Flag,
  Deleted_Datetime,
  Account_ID,
    CModuleState_ID,
    Trainee_ID,
    Invited_By_User_ID,
    UnInvited_By_User_ID,
    Course_ID,
    Module_ID,
    Status_ID,
    Module_Grade_Status_ID,
    Product_ID,
    First_Added_Date,
    First_Added_Time,
    First_Added_Datetime,
    Last_Modified_Date,
    Last_Modified_Time,
    Last_Modified_Datetime,
    Module_Started_Date,
    Module_Started_Time,
    Module_Started_Datetime,
    Invited_Date,
    Invited_Time,
    Invited_Datetime,
    Invitation_Email_Sent_Date,
    Invitation_Email_Sent_Time,
    Invitation_Email_Sent_Datetime,
    Invitation_Email_Responded_Date,
    Invitation_Email_Responded_Time,
    Invitation_Email_Responded_Datetime,
	UnInvited_Date,
	UnInvited_Time,
	UnInvited_Datetime,
    Reminder_Email_Sent_Date,
    Reminder_Email_Sent_Time,
    Reminder_Email_Sent_Datetime,
    Module_Completion_Date,
    Module_Completion_Time,
    Module_Completion_Datetime,
    Completion_Email_Sent_Date,
    Completion_Email_Sent_Time,
    Completion_Email_Sent_Datetime,
    Poke_Reminder_Email_Sent_Date,
    Poke_Reminder_Email_Sent_Time,
    Poke_Reminder_Email_Sent_Datetime,
    Trainee_Completion_Deadline_Date,
    Trainee_Completion_Deadline_Time,
    Trainee_Completion_Deadline_Datetime,
    Module_Last_Activity_Date,
    Module_Last_Activity_Time,
    Module_Last_Activity_Datetime,
    In_Progress_Complete_Percent,
    Module_Elapsed_Duration_Seconds,
    Module_Final_Grade_Percent,
    Module_Required_Flag,
    Took_Quiz_Flag,
    ETL_Run_Datetime
  
)
 

SELECT	Distinct		-- This is the Mapping between cc_CourseState and Module_Participation_Fact, with transformations ... mostly lookups against mtype references.

ETL_Datetime as Effective_Datetime,
null as Expiration_Datetime,
1 as Active_Flag,
Case When Deleted is not null Then 1 Else 0 End as Deleted_Flag,
ifnull(cc.deleted,'1900-01-01') as Deleted_Date,
cc.accountid as Account_ID,
cc.id as CModuleState_ID,
cc.userid as Trainee_ID,

ifnull(cc.invitedbyuserid,-1) as Invited_By_User_ID,
ifnull(cc.uninvitedbyuserid,-1) as UnInvited_By_User_ID,

cc.courseid as Course_ID,
cc.cmoduleid as Module_ID,
cc.status as Status_ID,
cc.gradestatus as Module_Grade_Status_ID,
cc.product as Product_ID,

cast(cc.created as date) as First_Added_Date,
cast(cc.created as time) as First_Added_Time,
cc.created as First_Added_Datetime,

cast(cc.modified as date) as  Last_Modified_By_Date,
cast(cc.modified as time) as Last_Modified_By_Time,
cc.modified as Last_Modified_By_Datetime,

cast(ifnull(cc.started,'1900-01-01') as date) as Module_Started_Date,
cast(ifnull(cc.started,-1) as time) as Module_Started_Time,
ifnull(cc.started,'1900-01-01') as Module_Started_Datetime,

cast(ifnull(cc.invited,'1900-01-01') as date) as Invited_Date,
cast(ifnull(cc.invited,-1) as time) as Invited_Time,
ifnull(cc.invited,'1900-01-01') as Invited_Datetime,

cast(ifnull(cc.invitationemailsent,'1900-01-01') as date) as    Invitation_Email_Sent_Date,
cast(ifnull(cc.invitationemailsent,-1) as time) as    Invitation_Email_Sent_Time,
ifnull(cc.invitationemailsent,'1900-01-01') as Invitation_Email_Sent_Datetime,

cast(ifnull(cc.invitationemailrespondeddate,'1900-01-01') as date)  as Invitation_Email_Responded_Date,
cast(ifnull(cc.invitationemailrespondeddate,-1) as time) as  Invitation_Email_Responded_Time,
ifnull(cc.invitationemailrespondeddate,'1900-01-01') as Invitation_Email_Responded_Datetime,

cast(ifnull(cc.uninvited,'1900-01-01') as date) as UnInvited_Date,
cast(ifnull(cc.uninvited,-1) as time) as UnInvited_Time,
ifnull(cc.uninvited,'1900-01-01') as UnInvited_Datetime,

cast(ifnull(cc.reminderemailsent,'1900-01-01') as date) as Reminder_Email_Sent_Date,
cast(ifnull(cc.reminderemailsent,-1) as time) as  Reminder_Email_Sent_Time,
ifnull(cc.reminderemailsent,'1900-01-01') as Reminder_Email_Sent_Datetime,

cast(ifnull(cc.completed,'1900-01-01') as date) as Module_Completion_Date,
cast(ifnull(cc.completed,-1) as time) as Module_Completion_Time,
ifnull(cc.completed,'1900-01-01') as Module_Completion_Datetime,

cast(ifnull(cc.completionemailsent,'1900-01-01') as date) as  Completion_Email_Sent_Date,
cast(ifnull(cc.completionemailsent,-1) as time) as  Completion_Email_Sent_Time,
ifnull(cc.completionemailsent,'1900-01-01')  as Completion_Email_Sent_Datetime,

cast(ifnull(cc.pokereminderemailsent,'1900-01-01') as date) as Poke_Reminder_Email_Sent_Date,
cast(ifnull(cc.pokereminderemailsent,-1) as time) as Poke_Reminder_Email_Sent_Time,
ifnull(cc.pokereminderemailsent,'1900-01-01') as Poke_Reminder_Email_Sent_Datetime,

cast(ifnull(cc.completiondeadline,'1900-01-01') as date) as Trainee_Completion_Deadline_Date,
cast(ifnull(cc.completiondeadline,-1) as time) as  Trainee_Completion_Deadline_Time,
ifnull(cc.completiondeadline,'1900-01-01') as Trainee_Completion_Deadline_Datetime,

cast(ifnull(cc.lastactivity,'1900-01-01') as date) as Module_Last_Activity_Date,
cast(ifnull(cc.lastactivity,-1) as time) as Module_Last_Activity_Time,
ifnull(cc.lastactivity,'1900-01-01') as  Module_Last_Activity_Datetime,

cc.progress as In_Progress_Complete_Percent,
cc.duration as Module_Elapsed_Duration_Seconds,
ifnull(cc.finalgrade,0) as Module_Final_Grade_Percent,
cc.required as Module_Required_Flag,
cc.hadquiz as Took_Quiz_Flag,
 
ETL_Datetime


From cc_CModuleState cc

left outer join Existing_Dim_IDs ED  -- Only insert records for Course IDs that do not previously exist in Module_Participation_Fact.
on cc.ID = ED.ID

left outer join TChange_Log_Ref clf
on cc.ID = clf.Source_Record_ID

-- You want to perform Inserts for all new cc_CourseState records, all changed existing records, but not existing records in cc_CourseState that did not change.
Where 

-- New Records.
ED.ID is Null

OR

-- Changed Records.
clf.Source_Record_ID is not null

;





/****************  UPDATE Change_Log ***************/
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
Inner Join Module_Participation_Fact d 				
On r.table_name = "CModuleState"
And r.source_record_id = d.cmodulestate_id
And d.active_flag = 1
Set r.Record_ID = d.MPF_ID    				-- This is the auto_increment record id.
;






/****************  CLEAN UP ANY TEMPORARY TABLES ***************/

Drop Table If Exists Existing_Dim_IDs;
Drop Table If Exists TChange_Log ; 
Drop Table If Exists TChange_Log_Ref_2 ;







COMMIT;

END$$
DELIMITER ;



-- End of script
