use pacifica; 

DROP PROCEDURE IF EXISTS Course_Participation_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Course_Participation_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN




-- DDL
/*
Drop Table If Exists Course_Participation_Fact ; 

Create Table Course_Participation_Fact

(CPF_ID bigint not null auto_increment,
 CourseState_ID bigint not null,

Deleted_Flag tinyint(1),
Deleted_Datetime datetime,
Effective_Datetime datetime,
Expiration_Datetime datetime,
Active_Flag int,

-- FKs
Account_ID  bigint,
Trainee_ID bigint,
Invited_By_User_ID bigint,
Course_ID  bigint,
Status_ID int(11) NOT NULL,
Course_Grade_Status_ID bigint,

-- Date / Time fields
First_Added_Date date,
First_Added_Time time,
First_Added_Datetime datetime,

Last_Modified_Date  Date  ,
Last_Modified_Time time ,
Last_Modified_Datetime  Datetime  ,

Course_Started_Date date,
Course_Started_Time time,
Course_Started_Datetime datetime,

Invited_Date date,
Invited_Time time,
Invited_Datetime datetime,

Invitation_Email_Sent_Date date,
Invitation_Email_Sent_Time time,
Invitation_Email_Sent_Datetime datetime,

Invitation_Email_Responded_Date date,
Invitation_Email_Responded_Time time,
Invitation_Email_Responded_Datetime datetime,

Reminder_Email_Sent_Date date,
Reminder_Email_Sent_Time time ,
Reminder_Email_Sent_Datetime datetime,

Course_Completion_Date date,
Course_Completion_Time time,
Course_Completion_Datetime datetime,

Completion_Email_Sent_Date date,
Completion_Email_Sent_Time time,
Completion_Email_Sent_Datetime datetime,

Poke_Reminder_Email_Sent_Date date,
Poke_Reminder_Email_Sent_Time time,
Poke_Reminder_Email_Sent_Datetime datetime,

Trainee_Completion_Deadline_Date date,
Trainee_Completion_Deadline_Time time,
Trainee_Completion_Deadline_Datetime datetime,

Course_Last_Activity_Date date,
Course_Last_Activity_Time time,
Course_Last_Activity_Datetime datetime,

In_Progress_Complete_Percent int ,
Course_Elapsed_Duration_Seconds int,
Course_Final_Grade_Percent int,
Course_Required_Flag tinyint(1),

ETL_Run_Datetime datetime,
  
  PRIMARY KEY (CPF_ID) 

) ENGINE=InnoDB DEFAULT CHARSET=utf8;

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
    Set sp_result = 0;

	-- This date serves as a "Batch Datetime" and links the cc data to the fact data.
	Set ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_CourseState ); 
    



/*************** Prepare some working tables ******************/

-- Get all of the existing Course_ID values from Course_Participation_Fact with the Extract Datetime.  We treat Effective_Datetime as a logical Batch ID.
Drop Table If Exists Existing_Dim_IDs;
Create temporary table Existing_Dim_IDs (ID bigint, Effective_Datetime datetime);
Insert Into Existing_Dim_IDs
Select distinct CourseState_ID, Effective_Datetime from Course_Participation_Fact where Active_Flag = 1; 



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
cc.ID as CourseState_ID,
cc.modified as Last_Modified_Datetime,
Case When ifnull(cc.deleted,'1900-01-01') <> fact.deleted_datetime Then 1 Else 0 End as Deleted_Date_Changed,
Case When cc.accountid <> fact.account_id Then 1 Else 0 End as Account_ID_Changed,
Case When cc.status <> fact.status_id Then 1 Else 0 End as Status_Changed,

-- Datetime fields are defaulted to 1900-01-01 when NULL. So we must compare the cc NULL value to this value in the fact table.
Case When ifnull(cc.invited,'1900-01-01') <> fact.invited_datetime  Then 1 Else 0 End as Invited_Datetime_Changed,
Case When ifnull(cc.invitationemailsent,'1900-01-01') <> fact.invitation_email_sent_datetime Then 1 Else 0 End as Invitation_Email_Sent_Changed,
Case When ifnull(cc.invitationemailrespondeddate,'1900-01-01') <> fact.invitation_email_responded_datetime Then 1 Else 0 End as Invite_Email_Responded_Changed,
Case When ifnull(cc.reminderemailsent,'1900-01-01') <> fact.reminder_email_sent_Datetime Then 1 Else 0 End as Reminder_Email_Sent_Changed,
Case When ifnull(cc.started,'1900-01-01') <> fact.course_started_datetime Then 1 Else 0 End as Course_Started_Changed,
Case When ifnull(cc.completed,'1900-01-01') <> fact.course_completion_datetime Then 1 Else 0 End as Course_Completed_Changed,
Case When ifnull(cc.completionemailsent,'1900-01-01') <> fact.completion_email_sent_datetime Then 1 Else 0 End as Completion_Email_Sent_Changed,
Case When ifnull(cc.pokereminderemailsent,'1900-01-01') <> fact.poke_reminder_email_sent_datetime Then 1 Else 0 End as Poke_Reminder_Email_Sent_Changed,
Case When ifnull(cc.lastactivity,'1900-01-01') <> fact.course_last_activity_datetime Then 1 Else 0 End as Course_Last_Activity_Changed,
Case When ifnull(cc.completiondeadline,'1900-01-01') <> fact.trainee_completion_deadline_datetime Then 1 Else 0 End as Completion_Deadline_Changed,

-- These are non-date change tests
Case When cc.finalgrade <> fact.course_final_grade_percent  Then 1 Else 0 End as Course_Final_Grade_Changed,
Case When cc.gradestatus <> fact.course_grade_status_id Then 1 Else 0 End as Course_Grade_Status_Changed,
Case When cc.required <> fact.course_required_flag Then 1 Else 0 End as Course_Required_Flag, 
Case When cc.duration <> fact.course_elapsed_duration_seconds Then 1 Else 0 End as Duration_Changed,
Case When cc.progress <> fact.in_progress_complete_percent  Then 1 Else 0 End as In_Progress_Changed,
Case When ifnull(cc.invitedbyuserid,-1) <> fact.invited_by_user_id  Then 1 Else 0 End as Invited_By_User_Changed


From cc_CourseState cc

Inner Join Course_Participation_Fact fact
On cc.id = fact.CourseState_ID

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
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
"Course_Started_Changed"
From TChange_Log 
Where Course_Started_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CourseState" ,
CourseState_ID, 
"Course_Completed_Changed"
From TChange_Log 
Where Course_Completed_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CourseState" ,
CourseState_ID, 
"Course_Final_Grade_Changed"
From TChange_Log 
Where Course_Final_Grade_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
"Course_Grade_Status_Changed"
From TChange_Log 
Where Course_Grade_Status_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CourseState" ,
CourseState_ID, 
"Course_Required_Flag"
From TChange_Log 
Where Course_Required_Flag = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CourseState" ,
CourseState_ID, 
"Course_Last_Activity_Changed"
From TChange_Log 
Where Course_Last_Activity_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
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
"CourseState" ,
CourseState_ID, 
"Invited_By_User_Changed"
From TChange_Log 
Where Invited_By_User_Changed = 1 ; 





/********************** PERFORM SLOWLY CHANGING DIMENSION UPDATES TO fact RECORDS ******************/

/*
The process steps are 
1. Identify cc_CourseState records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.
*/


-- These are your distinct IDs that are to be Up-Serted.   select distinct Source_Record_ID from TChange_Log_Ref


-- Expire old records that have changed.
UPDATE	Course_Participation_Fact ad
Inner Join TChange_Log_Ref cl 		-- these are your change capture records that are updates.
On cl.Source_Record_ID = ad.CourseState_ID
Set ad.Active_Flag = 0,
ad.Expiration_Datetime = ETL_Datetime 	-- Use the ETL_Datetime consistently for datetime references within this ETL process.
Where ad.Active_Flag = 1 ; 			-- For an Course, only expire the curretn Active record.







/****************************  PERFORM INSERTS AND UP-SERTS  **********************************/

/*
This step inserts NEW dimension ID records and inserts new Active records for existing IDs ( "Up-Sert" rather than an "Update" )

*/



-- Perform Insert for any new Courses.
INSERT INTO Course_Participation_Fact
( Effective_Datetime,
  Expiration_Datetime,
  Active_Flag,
  Deleted_Flag,
  Deleted_Datetime,
  Account_ID,
    CourseState_ID,
    Trainee_ID,
    Invited_By_User_ID,
    Course_ID,
    Status_ID,
    Course_Grade_Status_ID,
    First_Added_Date,
    First_Added_Time,
    First_Added_Datetime,
    Last_Modified_Date,
    Last_Modified_Time,
    Last_Modified_Datetime,
    Course_Started_Date,
    Course_Started_Time,
    Course_Started_Datetime,
    Invited_Date,
    Invited_Time,
    Invited_Datetime,
    Invitation_Email_Sent_Date,
    Invitation_Email_Sent_Time,
    Invitation_Email_Sent_Datetime,
    Invitation_Email_Responded_Date,
    Invitation_Email_Responded_Time,
    Invitation_Email_Responded_Datetime,
    Reminder_Email_Sent_Date,
    Reminder_Email_Sent_Time,
    Reminder_Email_Sent_Datetime,
    Course_Completion_Date,
    Course_Completion_Time,
    Course_Completion_Datetime,
    Completion_Email_Sent_Date,
    Completion_Email_Sent_Time,
    Completion_Email_Sent_Datetime,
    Poke_Reminder_Email_Sent_Date,
    Poke_Reminder_Email_Sent_Time,
    Poke_Reminder_Email_Sent_Datetime,
    Trainee_Completion_Deadline_Date,
    Trainee_Completion_Deadline_Time,
    Trainee_Completion_Deadline_Datetime,
    Course_Last_Activity_Date,
    Course_Last_Activity_Time,
    Course_Last_Activity_Datetime,
    In_Progress_Complete_Percent,
    Course_Elapsed_Duration_Seconds,
    Course_Final_Grade_Percent,
    Course_Required_Flag,
    ETL_Run_Datetime
  
)
 

SELECT	Distinct		-- This is the Mapping between cc_CourseState and Course_Participation_Fact, with transformations ... mostly lookups against mtype references.

ETL_Datetime as Effective_Datetime,
null as Expiration_Datetime,
1 as Active_Flag,
Case When Deleted is not null Then 1 Else 0 End as Deleted_Flag,
ifnull(cc.deleted,'1900-01-01') as Deleted_Date,
cc.accountid as Account_ID,
cc.id as CourseState_ID,
cc.userid as Trainee_ID,

ifnull(cc.invitedbyuserid,-1) as Invited_By_User_ID,

cc.courseid as Course_ID,
cc.status as Status_ID,
cc.gradestatus as Course_Grade_Status_ID,

cast(cc.created as date) as First_Added_Date,
cast(cc.created as time) as First_Added_Time,
cc.created as First_Added_Datetime,

cast(cc.modified as date) as  Last_Modified_By_Date,
cast(cc.modified as time) as Last_Modified_By_Time,
cc.modified as Last_Modified_By_Datetime,

cast(ifnull(cc.started,'1900-01-01') as date) as Course_Started_Date,
cast(ifnull(cc.started,-1) as time) as Course_Started_Time,
ifnull(cc.started,'1900-01-01') as Course_Started_Datetime,

cast(ifnull(cc.invited,'1900-01-01') as date) as Invited_Date,
cast(ifnull(cc.invited,-1) as time) as Invited_Time,
ifnull(cc.invited,'1900-01-01') as Invited_Datetime,

cast(ifnull(cc.invitationemailsent,'1900-01-01') as date) as    Invitation_Email_Sent_Date,
cast(ifnull(cc.invitationemailsent,-1) as time) as    Invitation_Email_Sent_Time,
ifnull(cc.invitationemailsent,'1900-01-01') as Invitation_Email_Sent_Datetime,

cast(ifnull(cc.invitationemailrespondeddate,'1900-01-01') as date)  as Invitation_Email_Responded_Date,
cast(ifnull(cc.invitationemailrespondeddate,-1) as time) as  Invitation_Email_Responded_Time,
ifnull(cc.invitationemailrespondeddate,'1900-01-01') as Invitation_Email_Responded_Datetime,

cast(ifnull(cc.reminderemailsent,'1900-01-01') as date) as Reminder_Email_Sent_Date,
cast(ifnull(cc.reminderemailsent,-1) as time) as  Reminder_Email_Sent_Time,
ifnull(cc.reminderemailsent,'1900-01-01') as Reminder_Email_Sent_Datetime,

cast(ifnull(cc.completed,'1900-01-01') as date) as Course_Completion_Date,
cast(ifnull(cc.completed,-1) as time) as Course_Completion_Time,
ifnull(cc.completed,'1900-01-01') as Course_Completion_Datetime,

cast(ifnull(cc.completionemailsent,'1900-01-01') as date) as  Completion_Email_Sent_Date,
cast(ifnull(cc.completionemailsent,-1) as time) as  Completion_Email_Sent_Time,
ifnull(cc.completionemailsent,'1900-01-01')  as Completion_Email_Sent_Datetime,

cast(ifnull(cc.pokereminderemailsent,'1900-01-01') as date) as Poke_Reminder_Email_Sent_Date,
cast(ifnull(cc.pokereminderemailsent,-1) as time) as Poke_Reminder_Email_Sent_Time,
ifnull(cc.pokereminderemailsent,'1900-01-01') as Poke_Reminder_Email_Sent_Datetime,

cast(ifnull(cc.completiondeadline,'1900-01-01') as date) as Trainee_Completion_Deadline_Date,
cast(ifnull(cc.completiondeadline,-1) as time) as  Trainee_Completion_Deadline_Time,
ifnull(cc.completiondeadline,'1900-01-01') as Trainee_Completion_Deadline_Datetime,

cast(ifnull(cc.lastactivity,'1900-01-01') as date) as Course_Last_Activity_Date,
cast(ifnull(cc.lastactivity,-1) as time) as Course_Last_Activity_Time,
ifnull(cc.lastactivity,'1900-01-01') as  Course_Last_Activity_Datetime,

cc.progress as In_Progress_Complete_Percent,
cc.duration as Course_Elapsed_Duration_Seconds,
ifnull(cc.finalgrade,0) as Course_Final_Grade_Percent,
cc.required as Course_Required_Flag,
 
ETL_Datetime


From cc_CourseState cc

left outer join Existing_Dim_IDs ED  -- Only insert records for Course IDs that do not previously exist in Course_Participation_Fact.
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
Inner Join Course_Participation_Fact d 				
On r.table_name = "CourseState"
And r.source_record_id = d.coursestate_id
And d.active_flag = 1
Set r.Record_ID = d.CPF_ID    				-- This is the auto_increment record id.
;






/****************  CLEAN UP ANY TEMPORARY TABLES ***************/

Drop Table If Exists Existing_Dim_IDs;
Drop Table If Exists TChange_Log ; 
Drop Table If Exists TChange_Log_Ref_2 ;







COMMIT;

END$$
DELIMITER ;



-- End of script
