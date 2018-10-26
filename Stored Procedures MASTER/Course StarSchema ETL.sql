use pacifica; 

DROP PROCEDURE IF EXISTS Course_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Course_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN


/********** BEGIN OF STORED PROCEDURE ***********/

/*
Source is cc_Course
Target is Course_Dim

Using an Effective_Datetime from the Change Capture Etl process to check for multiple "batch" loads.  This date is loaded into the Dim table when records are inserted or changed.
It serves as the Batch Run identifier and is compared to ensure duplicates are not inserted if the load was called twice in the same <time period>.  The time period can be 
adjusted.   Once the date changes it would allow the load to perform again.  This is an unlikely corner case, but could be improved if needed.

Approach for Slowly Changing Dimension is to Insert new records, and for changed records, 1. update the existing active 
record by changing it to INActive, and 2, inserting (UpSert) a new Active Record.   We never Update data (other than the SCD fields) on a record.

So the process steps are 
1. Identify cc_Course records that are Updates.  Log them.
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
    Set sp_result = 0;

	-- This date serves as a "Batch Datetime" and links the cc data to the dim data.
	Set ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_Course ); 
    


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

Drop Table if exists mtype_6;
Create Temporary Table mtype_6
(  ID int(11) NOT NULL,
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL 
) ;
Insert into mtype_6 Select ID, name from mtype_ref;

Drop Table if exists mtype_7;
Create Temporary Table mtype_7
(  ID int(11) NOT NULL,
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL 
) ;
Insert into mtype_7 Select ID, name from mtype_ref;

Drop Table if exists mtype_8;
Create Temporary Table mtype_8
(  ID int(11) NOT NULL,
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL 
) ;
Insert into mtype_8 Select ID, name from mtype_ref;

Drop Table if exists mtype_9;
Create Temporary Table mtype_9
(  ID int(11) NOT NULL,
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL 
) ;
Insert into mtype_9 Select ID, name from mtype_ref;

-- Get all of the existing Course_ID values from Course_Dim with the Extract Datetime.  We treat Effective_Datetime as a logical Batch ID.
Drop Table If Exists Existing_Dim_IDs;
Create temporary table Existing_Dim_IDs (ID bigint, Effective_Datetime datetime);
Insert Into Existing_Dim_IDs
Select distinct Course_ID, Effective_Datetime from Course_Dim where Active_Flag = 1; 





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
cc.ID as Course_ID,
cc.modified as Last_Modified_Datetime,
Case When cc.deleted <> dim.deleted_datetime Then 1 Else 0 End as Deleted_Date_Changed,
Case When cc.AccountID <> dim.Account_ID Then 1 Else 0 End as Account_ID_Changed,
Case When cc.name <> dim.Course_name Then 1 Else 0 End as Course_Name_Changed,
Case When cc.description <> dim.Course_description Then 1 Else 0 End as Course_Description_Changed,
Case When cc.type <> dim.type Then 1 Else 0 End as Course_Type_Changed,
Case When cc.status <> dim.status Then 1 Else 0 End as Course_Status_Changed,
Case When cc.ReminderInterval <> dim.ReminderInterval Then 1 Else 0 End as Reminder_Interval_Changed,
Case When cc.CertificateStatus <> dim.CertificateStatus Then 1 Else 0 End as Certificate_Status_Changed,
Case When cc.CertificateTemplate <> dim.CertificateTemplate Then 1 Else 0 End as Certificate_Template_Status_Changed,
Case When cc.CertificateQuizStatus <> dim.CertificateQuizStatus Then 1 Else 0 End as Certificate_Quiz_Status_Changed,
Case When cc.AccessType <> dim.AccessType Then 1 Else 0 End as Access_Type_Changed,
Case When cc.TrainerNotificationEmailsPeriod <> dim.TrainerNotificationEmailsPeriod Then 1 Else 0 End as Trainer_Notification_Emails_Period_Changed,
Case When cc.OwnerID <> dim.Owner_ID Then 1 Else 0 End as Owner_ID_Changed,
Case When cc.TrainerName <> dim.Trainer_Name Then 1 Else 0 End as Trainer_Name_Changed,
Case When cc.TrainerEmail <> dim.Trainer_Email Then 1 Else 0 End as Trainer_Email_Changed,
Case When cc.ReEnrollEnabled <> dim.ReEnroll_Enabled_Flag Then 1 Else 0 End as ReEnroll_Enabled_Flag_Changed,
Case When cc.QuizScoresMatter <> dim.Quiz_Scores_Matter_Flag Then 1 Else 0 End as Quiz_Scores_Matter_Flag_Changed,
Case When cc.CanContactTrainer <> dim.Can_Contact_Trainer_Flag Then 1 Else 0 End as Can_Contact_Trainer_Flag_Changed,
Case When cc.IsCoursePageActive <> dim.Is_Course_Page_Active_Flag Then 1 Else 0 End as Is_Course_Page_Active_Flag_Changed,
Case When cc.AllowTraineesToUnEnroll <> dim.Allow_Trainees_To_UnEnroll_Flag Then 1 Else 0 End as Allow_Trainees_To_UnEnroll_Flag_Changed,
Case When cc.EnrollmentExpirationDate <> dim.Enrollment_Expiration_Datetime Then 1 Else 0 End as Enrollment_Expiration_Datetime_Changed,
Case When cc.LastAccessed <> dim.Last_Accessed_Datetime Then 1 Else 0 End as Last_Accessed_Datetime_Changed,
Case When cc.IsEnrollmentExpirationActive <> dim.Enrollment_Expiration_Active_Flag Then 1 Else 0 End as Enrollment_Expiration_Active_Flag_Changed,
Case When cc.IsShowScoreOnCompletion <> dim.Show_Score_On_Completion_Flag Then 1 Else 0 End as Show_Score_On_Completion_Flag_Changed,
Case When cc.ReminderActive <> dim.Reminder_Active_Flag Then 1 Else 0 End as Reminder_Active_Flag_Changed,
Case When cc.StartBy <> dim.Start_By_Date Then 1 Else 0 End as Start_By_Date_Changed,
Case When cc.StartByDefined <> dim.Start_By_Defined_Flag Then 1 Else 0 End as Start_By_Defined_Flag_Changed,
Case When cc.ReTakeEnabled <> dim.ReTake_Enabled_Flag Then 1 Else 0 End as ReTake_Enabled_Flag_Changed,
Case When cc.CourseModified <> dim.Course_Modified_Datetime Then 1 Else 0 End as Course_Modified_Datetime_Changed,
Case When cc.ContentModified <> dim.Content_Modified_Datetime Then 1 Else 0 End as Content_Modified_Datetime_Changed,
Case When cc.CompletionDeadlineLength <> dim.Completion_Deadline_Length Then 1 Else 0 End as Completion_Deadline_Length_Changed,
Case When cc.PassingScore <> dim.Passing_Score Then 1 Else 0 End as Passing_Score_Changed,
Case When cc.HasPassingScore <> dim.Has_Passing_Score_Flag Then 1 Else 0 End as Has_Passing_Score_Flag_Changed,
Case When cc.CModuleOrderEnforced <> dim.Module_Order_Enforced_Flag Then 1 Else 0 End as Module_Order_Enforced_Flag_Changed,
Case When cc.CModuleFailureEnforced <> dim.Module_Failure_Enforced_Flag Then 1 Else 0 End as Module_Failure_Enforced_Flag_Changed,
Case When cc.CModuleCompletionEmail <> dim.Module_Completion_Email_Flag Then 1 Else 0 End as Module_Completion_Email_Flag_Changed,
Case When cc.YammerSharesEnabled <> dim.Yammer_Shares_Enabled_Flag Then 1 Else 0 End as Yammer_Shares_Enabled_Flag_Changed,
Case When cc.LastRemindAllNotStarted <> dim.Last_Remind_All_Not_Started_Datetime Then 1 Else 0 End as Last_Remind_All_Not_Started_Datetime_Changed,
Case When cc.LastRemindAllStarted <> dim.Last_Remind_All_Started_Datetime Then 1 Else 0 End as Last_Remind_All_Started_Datetime_Changed,
Case When cc.Language <> dim.Language Then 1 Else 0 End as Language_Changed,
Case When cc.AllowModuleRetake <> dim.Allow_Module_Retake_Flag Then 1 Else 0 End as Allow_Module_Retake_Flag_Changed,
Case When cc.AllowFastForward <> dim.Allow_Fast_Forward_Flag Then 1 Else 0 End as Allow_Fast_Forward_Flag_Changed,
Case When cc.UsePreviousScores <> dim.Use_Previous_Scores_Flag Then 1 Else 0 End as Use_Previous_Scores_Flag_Changed 

From cc_Course cc

Inner Join Course_dim dim
On cc.id = dim.Course_ID

Where dim.Active_Flag = 1     --  Only perform this change compare to the latest, active record in the Dim table.

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
"Course" ,
Course_ID, 
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
"Course" ,
Course_ID, 
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
"Course" ,
Course_ID, 
"Course Name Changed"
From TChange_Log 
Where Course_Name_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Course Description Changed"
From TChange_Log 
Where Course_Description_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Course_Type_Changed"
From TChange_Log 
Where Course_Type_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Course_Status_Changed"
From TChange_Log 
Where Course_Status_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Reminder_Interval_Changed"
From TChange_Log 
Where Reminder_Interval_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Certificate_Status_Changed"
From TChange_Log 
Where Certificate_Status_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Certificate_Template_Status_Changed"
From TChange_Log 
Where Certificate_Template_Status_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Certificate_Quiz_Status_Changed"
From TChange_Log 
Where Certificate_Quiz_Status_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Certificate_Quiz_Status_Changed"
From TChange_Log 
Where Certificate_Quiz_Status_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Access_Type_Changed"
From TChange_Log 
Where Access_Type_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Trainer_Notification_Emails_Period_Changed"
From TChange_Log 
Where Trainer_Notification_Emails_Period_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Owner_ID_Changed"
From TChange_Log 
Where  Owner_ID_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Trainer_Name_Changed"
From TChange_Log 
Where Trainer_Name_Changed = 1 ; 

 
Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Trainer_Email_Changed"
From TChange_Log 
Where Trainer_Email_Changed = 1 ; 

 
Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"ReEnroll_Enabled_Flag_Changed"
From TChange_Log 
Where ReEnroll_Enabled_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Quiz_Scores_Matter_Flag_Changed"
From TChange_Log 
Where Quiz_Scores_Matter_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Can_Contact_Trainer_Flag_Changed"
From TChange_Log 
Where Can_Contact_Trainer_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Is_Course_Page_Active_Flag_Changed"
From TChange_Log 
Where Is_Course_Page_Active_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Allow_Trainees_To_UnEnroll_Flag_Changed"
From TChange_Log 
Where Allow_Trainees_To_UnEnroll_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Allow_Trainees_To_UnEnroll_Flag_Changed"
From TChange_Log 
Where Allow_Trainees_To_UnEnroll_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Enrollment_Expiration_Datetime_Changed"
From TChange_Log 
Where Enrollment_Expiration_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Last_Accessed_Datetime_Changed"
From TChange_Log 
Where Last_Accessed_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Enrollment_Expiration_Active_Flag_Changed"
From TChange_Log 
Where Enrollment_Expiration_Active_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Show_Score_On_Completion_Flag_Changed"
From TChange_Log 
Where Show_Score_On_Completion_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Reminder_Active_Flag_Changed"
From TChange_Log 
Where Reminder_Active_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Start_By_Date_Changed"
From TChange_Log 
Where Start_By_Date_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Start_By_Defined_Flag_Changed"
From TChange_Log 
Where Start_By_Defined_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"ReTake_Enabled_Flag_Changed"
From TChange_Log 
Where ReTake_Enabled_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Course_Modified_Datetime_Changed"
From TChange_Log 
Where Course_Modified_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Content_Modified_Datetime_Changed"
From TChange_Log 
Where Content_Modified_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Completion_Deadline_Length_Changed"
From TChange_Log 
Where Completion_Deadline_Length_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Passing_Score_Changed"
From TChange_Log 
Where Passing_Score_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Has_Passing_Score_Flag_Changed"
From TChange_Log 
Where Has_Passing_Score_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Module_Order_Enforced_Flag_Changed"
From TChange_Log 
Where Module_Order_Enforced_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Module_Failure_Enforced_Flag_Changed"
From TChange_Log 
Where Module_Failure_Enforced_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Module_Completion_Email_Flag_Changed"
From TChange_Log 
Where Module_Completion_Email_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Yammer_Shares_Enabled_Flag_Changed"
From TChange_Log 
Where Yammer_Shares_Enabled_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Last_Remind_All_Not_Started_Datetime_Changed"
From TChange_Log 
Where Last_Remind_All_Not_Started_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Last_Remind_All_Started_Datetime_Changed"
From TChange_Log 
Where Last_Remind_All_Started_Datetime_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Language_Changed"
From TChange_Log 
Where Language_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Allow_Module_Retake_Flag_Changed"
From TChange_Log 
Where Allow_Module_Retake_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Allow_Fast_Forward_Flag_Changed"
From TChange_Log 
Where Allow_Fast_Forward_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Course" ,
Course_ID, 
"Use_Previous_Scores_Flag_Changed"
From TChange_Log 
Where Use_Previous_Scores_Flag_Changed = 1 ; 






/********************** PERFORM SLOWLY CHANGING DIMENSION UPDATES TO DIM RECORDS ******************/

/*
The process steps are 
1. Identify cc_Course records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.
*/


-- These are your distinct IDs that are to be Up-Serted.   select distinct Source_Record_ID from TChange_Log_Ref


-- Expire old records that have changed.
UPDATE	Course_Dim ad
Inner Join TChange_Log_Ref cl 		-- these are your change capture records that are updates.
On cl.Source_Record_ID = ad.Course_ID
Set ad.Active_Flag = 0,
ad.Expiration_Datetime = ETL_Datetime 	-- Use the ETL_Datetime consistently for datetime references within this ETL process.
Where ad.Active_Flag = 1 ; 			-- For an Course, only expire the curretn Active record.








/****************************  PERFORM INSERTS AND UP-SERTS  **********************************/

/*
This step inserts NEW dimension ID records and inserts new Active records for existing IDs ( "Up-Sert" rather than an "Update" )

*/



-- Perform Insert for any new Courses.
INSERT INTO Course_dim
( Effective_Datetime,
  Expiration_Datetime,
  Active_Flag,
  Deleted_Flag,
  Deleted_Datetime,
  Course_ID,
  Account_ID,
  First_Added_Datetime,
  Last_Modified_Datetime,
  Owner_ID ,
  Course_Type  ,			-- mtype
  Course_Name ,
  Course_Description ,
  Course_Status ,		-- mtype
  Trainer_Name ,
  Trainer_Email ,
  ReEnroll_Enabled_Flag ,
  Quiz_Scores_Matter_Flag ,
  Can_Contact_Trainer_Flag ,
  Is_Course_Page_Active_Flag ,
  Allow_Trainees_To_UnEnroll_Flag ,
  Enrollment_Expiration_Datetime ,
  Last_Accessed_Datetime ,
  Enrollment_Expiration_Active_Flag ,
  Show_Score_On_Completion_Flag ,
  Reminder_Active_Flag ,
  Reminder_Interval  ,			-- mtype
  Start_By_Date ,
  Start_By_Defined_Flag ,
  ReTake_Enabled_Flag ,
  Certificate_Status  ,			-- mtype
  Certificate_Template  ,		-- mtype
  Certificate_Quiz_Status ,			-- mtype
  Access_Type ,			-- mtype
  Trainer_Notification_Datetime ,
  Trainer_Notification_Emails_Period   ,			-- mtype
  Course_Modified_Datetime ,
  Content_Modified_Datetime ,
  Completion_Deadline_Length,
  Passing_Score ,
  Has_Passing_Score_Flag ,
  Module_Order_Enforced_Flag ,
  Module_Failure_Enforced_Flag ,
  Module_Completion_Email_Flag ,
  Yammer_Shares_Enabled_Flag ,
  Last_Remind_All_Not_Started_Datetime ,
  Last_Remind_All_Started_Datetime ,
  Language ,
  Allow_Module_Retake_Flag ,
  Allow_Fast_Forward_Flag ,
  Use_Previous_Scores_Flag ,

  -- Admin 
  Type ,
  Status ,
  ReminderInterval ,
  CertificateStatus ,
  CertificateTemplate ,
  CertificateQuizStatus , 
  AccessType ,
  TrainerNotificationEmailsPeriod ,  
  ETL_Run_Datetime
  
)


SELECT	Distinct		-- This is the Mapping between cc_Course and Course_dim, with transformations ... mostly lookups against mtype references.

ETL_Datetime as Effective_Datetime,
null as Expiration_Datetime,
1 as Active_Flag,
Case When Deleted is not null Then 1 Else 0 End as Deleted_Flag,
cc.deleted as Deleted_Date,
cc.ID as Course_ID,
cc.accountid as Account_ID,
cc.Created as First_Added_Date,
cc.Modified as Last_Modified_Date,

cc.ownerid as Owner_ID ,

m2.name as Course_Type  ,			-- mtype
cc.name as Course_Name,
cc.description as Course_Description ,
m3.name as Course_Status ,		-- mtype
cc.trainername as Trainer_Name ,
cc.traineremail as  Trainer_Email ,
cc.reenrollenabled as ReEnroll_Enabled_Flag ,
cc.quizscoresmatter as Quiz_Scores_Matter_Flag ,
cc.cancontacttrainer as  Can_Contact_Trainer_Flag ,
cc.iscoursepageactive as Is_Course_Page_Active_Flag ,
cc.allowtraineestounenroll as  Allow_Trainees_To_UnEnroll_Flag ,
cc.enrollmentexpirationdate as  Enrollment_Expiration_Datetime ,
cc.lastaccessed as Last_Accessed_Datetime ,
cc.isenrollmentexpirationactive as Enrollment_Expiration_Active_Flag ,
cc.isshowscoreoncompletion as  Show_Score_On_Completion_Flag ,
cc.reminderactive as Reminder_Active_Flag ,
m4.name as Reminder_Interval  ,			-- mtype
cc.startby as Start_By_Date ,
cc.startbydefined as Start_By_Defined_Flag ,
cc.retakeenabled as ReTake_Enabled_Flag ,
m5.name as   Certificate_Status  ,			-- mtype
m6.name as Certificate_Template  ,		-- mtype
m7.name as Certificate_Quiz_Status ,			-- mtype
m8.name as Access_Type ,			-- mtype
cc.trainernotificationdate as Trainer_Notification_Datetime ,
m9.name as  Trainer_Notification_Emails_Period   ,			-- mtype
cc.coursemodified as Course_Modified_Datetime ,
cc.contentmodified as Content_Modified_Datetime ,
cc.CompletionDeadlineLength,
cc.passingscore as Passing_Score ,
cc.haspassingscore as Has_Passing_Score_Flag ,
cc.cmoduleorderenforced as   Module_Order_Enforced_Flag ,
cc.cmodulefailureenforced as  Module_Failure_Enforced_Flag ,
cc.cmodulecompletionemail as  Module_Completion_Email_Flag ,
cc.yammersharesenabled as Yammer_Shares_Enabled_Flag ,
cc.lastremindallnotstarted as Last_Remind_All_Not_Started_Datetime ,
cc.lastremindallstarted as Last_Remind_All_Started_Datetime ,
cc.Language as Language,
cc.allowmoduleretake as   Allow_Module_Retake_Flag ,
cc.allowfastforward as  Allow_Fast_Forward_Flag ,
cc.usepreviousscores as Use_Previous_Scores_Flag ,

-- Admin 
cc.Type ,
cc.Status ,
cc.ReminderInterval ,
cc.CertificateStatus ,
cc.CertificateTemplate ,
cc.CertificateQuizStatus , 
cc.AccessType ,
cc.TrainerNotificationEmailsPeriod ,  
ETL_Datetime

-- Admin Fields

From cc_Course cc

left outer join mtype_2 m2
on cc.type = m2.id


left outer join mtype_3 m3
on cc.status = m3.id


left outer join mtype_4 m4
on cc.ReminderInterval = m4.id


left outer join mtype_5 m5
on cc.CertificateStatus = m5.id


left outer join mtype_6 m6
on cc.CertificateTemplate = m6.id


left outer join mtype_7 m7
on cc.CertificateQuizStatus = m7.id


left outer join mtype_8 m8
on cc.AccessType = m8.id


left outer join mtype_9 m9
on cc.TrainerNotificationEmailsPeriod = m9.id


left outer join Existing_Dim_IDs ED  -- Only insert records for Course IDs that do not previously exist in Course_Dim.
on cc.ID = ED.ID

left outer join TChange_Log_Ref clf
on cc.ID = clf.Source_Record_ID

-- You want to perform Inserts for all new cc_Course records, all changed existing records, but not existing records in cc_Course that did not change.
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
Inner Join Course_Dim d 				
On r.table_name = "Course"
And r.source_record_id = d.course_id
And d.active_flag = 1
Set r.Record_ID = d.ID    				-- This is the auto_increment record id.
;






/****************  CLEAN UP ANY TEMPORARY TABLES ***************/

Drop Table if exists mtype_2;
Drop Table if exists mtype_3;
Drop Table if exists mtype_4;
Drop Table if exists mtype_5;
Drop Table if exists mtype_6;
Drop Table if exists mtype_7;
Drop Table if exists mtype_8;
Drop Table if exists mtype_9;
Drop Table If Exists Existing_Dim_IDs;
Drop Table If Exists TChange_Log ; 
Drop Table If Exists TChange_Log_Ref_2 ;







COMMIT;

END$$
DELIMITER ;



-- End of script

