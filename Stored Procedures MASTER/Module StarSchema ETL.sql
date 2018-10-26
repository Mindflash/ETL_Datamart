use pacifica; 

DROP PROCEDURE IF EXISTS Module_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Module_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN


/********** BEGIN OF STORED PROCEDURE ***********/

/*
Source is cc_Module
Target is Module_Dim

Using an Effective_Datetime from the Change Capture Etl process to check for multiple "batch" loads.  This date is loaded into the Dim table when records are inserted or changed.
It serves as the Batch Run identifier and is compared to ensure duplicates are not inserted if the load was called twice in the same <time period>.  The time period can be 
adjusted.   Once the date changes it would allow the load to perform again.  This is an unlikely corner case, but could be improved if needed.

Approach for Slowly Changing Dimension is to Insert new records, and for changed records, 1. update the existing active 
record by changing it to INActive, and 2, inserting (UpSert) a new Active Record.   We never Update data (other than the SCD fields) on a record.

So the process steps are 
1. Identify cc_Module records that are Updates.  Log them.
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
	Set ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_CModule ); 
    
    


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

Drop Table if exists mtype_10;
Create Temporary Table mtype_10
(  ID int(11) NOT NULL,
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL 
) ;
Insert into mtype_10 Select ID, name from mtype_ref;


-- Get all of the existing Module_ID values from Module_Dim with the Extract Datetime.  We treat Effective_Datetime as a logical Batch ID.
Drop Table If Exists Existing_Dim_IDs;
Create temporary table Existing_Dim_IDs (ID bigint, Effective_Datetime datetime);
Insert Into Existing_Dim_IDs
Select distinct Module_ID, Effective_Datetime from Module_Dim where Active_Flag = 1; 



-- Get Module names for the Module_Copied_From fields. There are two.
Drop Table If Exists Module_Names ;
Create Temporary Table Module_Names (ID bigint, Name varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci );
Insert into Module_Names
Select Distinct ID , substring(Name,1,100) as mname From CModule ;

Drop Table If Exists Module_Names_2 ;
Create Temporary Table Module_Names_2 (ID bigint, Name varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci );
Insert into Module_Names_2
Select * From Module_Names;

Create Index NameID On Module_Names (Id); 
Create Index NameID On Module_Names_2 (Id); 





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
cc.ID as Module_ID,
cc.modified as Last_Modified_Datetime,
Case When cc.deleted <> dim.deleted_datetime Then 1 Else 0 End as Deleted_Date_Changed,
Case When cc.AccountID <> dim.Account_ID Then 1 Else 0 End as Account_ID_Changed,
Case When cc.name <> dim.Module_name Then 1 Else 0 End as Module_Name_Changed,
Case When cc.description <> dim.Module_description Then 1 Else 0 End as Module_Description_Changed,
Case When cc.type <> dim.type Then 1 Else 0 End as Module_Type_Changed,
Case When cc.status <> dim.status Then 1 Else 0 End as Module_Status_Changed,
Case When cc.ownerid <> dim.Owner_ID Then 1 Else 0 End as Owner_ID_Changed,
Case When cc.trainername <> dim.Trainer_Name Then 1 Else 0 End as Trainer_Name_Changed,
Case When cc.traineremail <> dim.Trainer_Email Then 1 Else 0 End as Trainer_Email_Changed,
Case When cc.reenrollenabled <> dim.ReEnroll_Enabled_Flag Then 1 Else 0 End as ReEnroll_Enabled_Changed,
Case When cc.quizscoresmatter <> dim.Quiz_Scores_Matter_Flag Then 1 Else 0 End as Quiz_Scores_Matter_Changed,
Case When cc.cancontacttrainer <> dim.Can_Contact_Trainer_Flag  Then 1 Else 0 End as Can_Contact_Trainer_Changed,
Case When cc.iscmodulepageactive <> dim.Module_Page_Active_Flag  Then 1 Else 0 End as Module_Page_Active_Changed,
Case When cc.allowtraineestounenroll <> dim.Allow_Trainees_To_UnEnroll_Flag  Then 1 Else 0 End as  Allow_Trainees_UnEnroll_Changed,
Case When cc.lastaccessed <> dim.Last_Accessed_Datetime  Then 1 Else 0 End as Last_Accessed_Changed,
Case When cc.isshowscoreoncompletion <> dim.Show_Score_On_Completion_Flag  Then 1 Else 0 End as Show_On_Completion_Changed,
Case When cc.reminderactive <> dim.Reminder_Active_Flag  Then 1 Else 0 End as Reminder_Active_Changed,
Case When cc.ReminderInterval <> dim.ReminderInterval Then 1 Else 0 End as Reminder_Interval_Changed,
Case When cc.startby <> dim.Start_By_Date Then 1 Else 0 End as Start_By_Changed,
Case When cc.startbydefined <> dim.Start_By_Defined_Flag Then 1 Else 0 End as Start_By_Defined_Changed,
Case When cc.retakeenabled <> dim.ReTake_Enabled_Flag  Then 1 Else 0 End as ReTake_Enabled_Changed,
Case When cc.certificatestatus <> dim.certificatestatus  Then 1 Else 0 End as Certificate_Status_Changed,
Case When cc.certificatetemplate <> dim.certificatetemplate  Then 1 Else 0 End as Certificate_Template_Changed,
Case When cc.certificatequizstatus <> dim.certificatequizstatus  Then 1 Else 0 End as Certificate_Quiz_Status_Changed,
Case When cc.accesstype <> dim.accesstype  Then 1 Else 0 End as Access_Type_Changed,
Case When cc.trainernotificationdate <> dim.Trainer_Notification_Datetime  Then 1 Else 0 End as Trainer_Notification_Changed,
Case When cc.trainernotificationemailsperiod <> dim.trainernotificationemailsperiod  Then 1 Else 0 End as Trainer_Notifications_Email_Period_Changed,
Case When cc.cmodulemodified <> dim.Module_Modified_Datetime  Then 1 Else 0 End as Module_Modified_Changed,
Case When cc.contentmodified <> dim.Content_Modified_Datetime  Then 1 Else 0 End as Content_Modified_Changed,
Case When cc.completiondeadlinelength <> dim.Completion_Deadline_Length  Then 1 Else 0 End as Completion_Deadline_Length_Changed,
Case When cc.completiondeadlinetimeunit <> dim.completiondeadlinetimeunit  Then 1 Else 0 End as Completion_Deadline_Time_Unit_Changed,
Case When cc.passingscore <> dim.Passing_Score Then 1 Else 0 End as Passing_Score_Changed,
Case When cc.haspassingscore <> dim.Has_Passing_Score_Flag Then 1 Else 0 End as Has_Passing_Score_Changed,
Case When cc.copiedfromcmoduleid <> dim.Copied_From_Module_ID Then 1 Else 0 End as Copied_From_Module_ID_Changed,
Case When cc.copiedfromsamplecmoduleid <> dim.Copied_From_Sample_Module_ID Then 1 Else 0 End as Copied_From_Sample_Module_ID_Changed,
Case When cc.showinmarketplacecatalog <> dim.Show_In_Marketplace_Catalog_Flag Then 1 Else 0 End as Show_In_MarketPlace_Catalog_Changed,
Case When cc.bannedfrommarkeplace <> dim.Banned_From_Markeplace_Datetime Then 1 Else 0 End as Banned_From_MarketPlace_Changed,
Case When cc.lastremindallnotstarted <> dim.Last_Remind_All_Not_Started_Datetime Then 1 Else 0 End as Last_Remind_All_Not_Started_Changed,
Case When cc.lastremindallstarted <> dim.Last_Remind_All_Started_Datetime Then 1 Else 0 End as Last_Remind_All_Started_Datetime_Changed,
Case When cc.language <> dim.language Then 1 Else 0 End as Language_Changed,
Case When cc.disablefastforward <> dim.Disable_Fast_Forward Then 1 Else 0 End as Disabled_Fast_Forward_Changed
 
  
From cc_CModule cc

Inner Join Module_dim dim
On cc.id = dim.Module_ID

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
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
"Module Name Changed"
From TChange_Log 
Where Module_Name_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Module Description Changed"
From TChange_Log 
Where Module_Description_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Module_Type_Changed"
From TChange_Log 
Where Module_Type_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Module_Status_Changed"
From TChange_Log 
Where Module_Status_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Owner_ID_Changed"
From TChange_Log 
Where Owner_ID_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
"ReEnroll_Enabled_Changed"
From TChange_Log 
Where ReEnroll_Enabled_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Quiz_Scores_Matter_Changed"
From TChange_Log 
Where Quiz_Scores_Matter_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Can_Contact_Trainer_Changed"
From TChange_Log 
Where Can_Contact_Trainer_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Module_Page_Active_Changed"
From TChange_Log 
Where Module_Page_Active_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Allow_Trainees_UnEnroll_Changed"
From TChange_Log 
Where Allow_Trainees_UnEnroll_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Last_Accessed_Changed"
From TChange_Log 
Where Last_Accessed_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Show_On_Completion_Changed"
From TChange_Log 
Where Show_On_Completion_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Reminder_Active_Changed"
From TChange_Log 
Where Reminder_Active_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
"Start_By_Changed"
From TChange_Log 
Where Start_By_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Start_By_Defined_Changed"
From TChange_Log 
Where Start_By_Defined_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"ReTake_Enabled_Changed"
From TChange_Log 
Where ReTake_Enabled_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
"Certificate_Template_Changed"
From TChange_Log 
Where Certificate_Template_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
"Trainer_Notification_Changed"
From TChange_Log 
Where Trainer_Notification_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Trainer_Notification_Changed"
From TChange_Log 
Where Trainer_Notification_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Trainer_Notifications_Email_Period_Changed"
From TChange_Log 
Where Trainer_Notifications_Email_Period_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Module_Modified_Changed"
From TChange_Log 
Where Module_Modified_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Content_Modified_Changed"
From TChange_Log 
Where Content_Modified_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
"Completion_Deadline_Time_Unit_Changed"
From TChange_Log 
Where Completion_Deadline_Time_Unit_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
"Has_Passing_Score_Changed"
From TChange_Log 
Where Has_Passing_Score_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Copied_From_Module_ID_Changed"
From TChange_Log 
Where Copied_From_Module_ID_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Copied_From_Sample_Module_ID_Changed"
From TChange_Log 
Where Copied_From_Sample_Module_ID_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Show_In_MarketPlace_Catalog_Changed"
From TChange_Log 
Where Show_In_MarketPlace_Catalog_Changed = 1 ;  



Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Banned_From_MarketPlace_Changed"
From TChange_Log 
Where Banned_From_MarketPlace_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
"Last_Remind_All_Not_Started_Changed"
From TChange_Log 
Where Last_Remind_All_Not_Started_Changed = 1 ;  


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
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
"Module" ,
Module_ID, 
"Disabled_Fast_Forward_Changed"
From TChange_Log 
Where Disabled_Fast_Forward_Changed = 1 ;  



  


/********************** PERFORM SLOWLY CHANGING DIMENSION UPDATES TO DIM RECORDS ******************/

/*
The process steps are 
1. Identify cc_Module records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.
*/


-- These are your distinct IDs that are to be Up-Serted.   select distinct Source_Record_ID from TChange_Log_Ref


-- Expire old records that have changed.
UPDATE	Module_Dim ad
Inner Join TChange_Log_Ref cl 		-- these are your change capture records that are updates.
On cl.Source_Record_ID = ad.Module_ID
Set ad.Active_Flag = 0,
ad.Expiration_Datetime =ETL_Datetime 	-- Use theETL_Datetime consistently for datetime references within this ETL process.
Where ad.Active_Flag = 1 ; 			-- For an Module, only expire the curretn Active record.







/****************************  PERFORM INSERTS AND UP-SERTS  **********************************/

/*
This step inserts NEW dimension ID records and inserts new Active records for existing IDs ( "Up-Sert" rather than an "Update" )

*/



-- Perform Insert for any new Modules.
INSERT INTO Module_dim
   
(   Effective_Datetime,
    Expiration_Datetime,
    Active_Flag,
    Deleted_Flag,
    Deleted_Datetime,
    Module_ID,
    Account_ID,
    First_Added_Datetime,
    Last_Modified_Datetime,
    Owner_ID,
    Module_Type,
    Module_Name,
    Module_Description,
    Module_Status,
    Trainer_Name,
    Trainer_Email,
    ReEnroll_Enabled_Flag,
    Quiz_Scores_Matter_Flag,
    Can_Contact_Trainer_Flag,
    Module_Page_Active_Flag,
    Allow_Trainees_To_UnEnroll_Flag,
    Last_Accessed_Datetime,
    Show_Score_On_Completion_Flag,
    Reminder_Active_Flag,
    Reminder_Interval,
    Start_By_Date,
    Start_By_Defined_Flag,
    ReTake_Enabled_Flag,
    Certificate_Status,
    Certificate_Template,
    Certificate_Quiz_Status,
    Access_Type,
    Trainer_Notification_Datetime,
    Trainer_Notification_Emails_Period,
    Module_Modified_Datetime,
    Content_Modified_Datetime,
    Completion_Deadline_Length,
    Completion_Deadline_Time_Unit,
    Passing_Score,
    Has_Passing_Score_Flag,
    Copied_From_Module_Name,
    Copied_From_Module_ID,
    Copied_From_Sample_Module_Name,
    Copied_From_Sample_Module_ID,
    Show_In_Marketplace_Catalog_Flag,
    Banned_From_Markeplace_Datetime,
    Last_Remind_All_Not_Started_Datetime,
    Last_Remind_All_Started_Datetime,
    Language,
    Disable_Fast_Forward,

	-- Admin 
    Type,
    Status,
    ReminderInterval,
    CertificateStatus,
    CertificateTemplate,
    CertificateQuizStatus,
    AccessType,
    TrainerNotificationEmailsPeriod,
    CompletionDeadlineTimeUnit,
    Product,
    ETL_Run_Datetime
)



SELECT	Distinct		-- This is the Mapping between cc_Module and Module_dim, with transformations ... mostly lookups against mtype references.

ETL_Datetime as Effective_Datetime,
null as Expiration_Datetime,
1 as Active_Flag,
Case When Deleted is not null Then 1 Else 0 End as Deleted_Flag,
cc.deleted as Deleted_Date,
cc.ID as Module_ID,
cc.accountid as Account_ID,
cc.Created as First_Added_Date,
cc.Modified as Last_Modified_Date,
cc.ownerid as Owner_ID ,
m2.name as Module_Type  ,			-- mtype
cc.name as Module_Name,
cc.description as Module_Description ,
m3.name as Module_Status ,		-- mtype
cc.trainername as Trainer_Name ,
cc.traineremail as  Trainer_Email ,
cc.reenrollenabled as ReEnroll_Enabled_Flag ,
cc.quizscoresmatter as Quiz_Scores_Matter_Flag ,
cc.cancontacttrainer as  Can_Contact_Trainer_Flag ,
cc.iscmodulepageactive as Is_Module_Page_Active_Flag ,
cc.allowtraineestounenroll as  Allow_Trainees_To_UnEnroll_Flag ,
cc.lastaccessed as Last_Accessed_Datetime ,
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
cc.cmodulemodified as Module_Modified_Datetime ,
cc.contentmodified as Content_Modified_Datetime ,
cc.CompletionDeadlineLength as Completion_Deadline_Length,
m10.name as Completion_Deadline_Time_Unit,
cc.passingscore as Passing_Score ,
cc.haspassingscore as Has_Passing_Score_Flag ,
mn.name as Copied_From_Module_Name ,
cc.copiedfromcmoduleid as  Copied_From_Module_ID ,
mn2.name as Copied_From_Sample_Module_Name ,
cc.copiedfromsamplecmoduleid as Copied_From_Sample_Module_ID ,
cc.showinmarketplacecatalog as Show_In_Marketplace_Catalog_Flag ,
cc.bannedfrommarkeplace as Banned_From_Markeplace_Datetime ,
cc.lastremindallnotstarted as Last_Remind_All_Not_Started_Datetime ,
cc.lastremindallstarted as Last_Remind_All_Started_Datetime ,
cc.language as Language ,
cc.disablefastforward as Disable_Fast_Forward ,

	-- Admin 
cc.Type ,
cc.Status,
cc.ReminderInterval,
cc.CertificateStatus,
cc.CertificateTemplate,
cc.CertificateQuizStatus,
cc.AccessType,
cc.TrainerNotificationEmailsPeriod,
cc.CompletionDeadlineTimeUnit, 
cc.Product, 
ETL_Datetime

From cc_CModule cc

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


left outer join mtype_10 m10
on cc.CompletionDeadlineTimeUnit = m10.id


left outer join Module_Names  mn            -- Copied_From_Module_Name
on cc.CopiedFromCModuleID  = mn.id


left outer join Module_Names_2  mn2            -- Copied_From_Sample_Module_Name
on cc.CopiedFromSampleCModuleID  = mn2.id
  

left outer join Existing_Dim_IDs ED  -- Only insert records for Module IDs that do not previously exist in Module_Dim.
on cc.ID = ED.ID

left outer join TChange_Log_Ref clf
on cc.ID = clf.Source_Record_ID

-- You want to perform Inserts for all new cc_Module records, all changed existing records, but not existing records in cc_Module that did not change.
Where 

-- New Records.
ED.ID is Null

OR

-- Changed Records.
clf.Source_Record_ID is not null

;







/****************  UPDATE Change_Log ***************/
-- Need to ensure this only occurs once per logical batch.  Perform near end of Dim Load Process.

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
Inner Join Module_Dim d 				
On r.table_name = "Module"
And r.source_record_id = d.module_id
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
Drop Table if exists mtype_10;
Drop Table if exists module_name;
Drop Table if exists module_name_2;
Drop Table If Exists Existing_Dim_IDs;
Drop Table If Exists TChange_Log ; 
Drop Table If Exists TChange_Log_Ref_2 ;





COMMIT;

END$$
DELIMITER ;



-- End of script

