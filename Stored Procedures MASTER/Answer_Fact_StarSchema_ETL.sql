use pacifica; 

DROP PROCEDURE IF EXISTS Answer_Fact_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Answer_Fact_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
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
	Set ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_FrameState ); 
    



/*************** Prepare some working tables ******************/

-- Get all of the existing Framestate_ID values from Answer_Fact_Fact with the Extract Datetime.  We treat Effective_Datetime as a logical Batch ID.
Drop Table If Exists Existing_Dim_IDs;
Create temporary table Existing_Dim_IDs (ID bigint, Effective_Datetime datetime);
Insert Into Existing_Dim_IDs
Select distinct FrameState_ID, Effective_Datetime from Answer_Fact where Active_Flag = 1; 





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
cc.FrameStateID as FrameState_ID,
cc.modified as Last_Modified_Datetime,
Case When ifnull(cc.deleted,'1900-01-01') <> fact.deleted_datetime Then 1 Else 0 End as Deleted_Date_Changed,

Case When cc.accountid <> fact.account_id Then 1 Else 0 End as Account_ID_Changed,

Case When cc.quizid  <> fact.Quiz_ID Then 1 Else 0 End as Quiz_ID_Changed,

Case When cc.cmoduleid  <> fact.Module_ID Then 1 Else 0 End as CModule_ID_Changed,

Case When cc.courseid  <> fact.Course_ID Then 1 Else 0 End as Course_ID_Changed,
 
Case When cc.Status <> fact.Status_ID  Then 1 Else 0 End as Status_Changed,

Case When cc.EssayAnswerLength <> fact.Essay_Answer_Length  Then 1 Else 0 End as Essay_Answer_Length_Changed


From cc_FrameState cc

Inner Join Answer_Fact fact
On cc.FrameStateid = fact.FrameState_ID

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
"FrameState" ,
FrameState_ID, 
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
"FrameState" ,
FrameState_ID, 
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
"FrameState" ,
FrameState_ID, 
"Quiz_ID_Changed"
From TChange_Log 
Where Quiz_ID_Changed = 1 ; 



Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"FrameState" ,
FrameState_ID, 
"CModule_ID_Changed"
From TChange_Log 
Where CModule_ID_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"FrameState" ,
FrameState_ID, 
"Course_ID_Changed"
From TChange_Log 
Where Course_ID_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"FrameState" ,
FrameState_ID, 
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
"FrameState" ,
FrameState_ID, 
"Essay_Answer_Length_Changed"
From TChange_Log 
Where Essay_Answer_Length_Changed = 1 ; 



/********************** PERFORM SLOWLY CHANGING DIMENSION UPDATES TO fact RECORDS ******************/

/*
The process steps are 
1. Identify cc_CourseState records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.
*/


-- These are your distinct IDs that are to be Up-Serted.   select distinct Source_Record_ID from TChange_Log_Ref


-- Expire old records that have changed.
UPDATE	Answer_Fact ad
Inner Join TChange_Log_Ref cl 		-- these are your change capture records that are updates.
On cl.Source_Record_ID = ad.FrameState_ID
Set ad.Active_Flag = 0,
ad.Expiration_Datetime = ETL_Datetime 	-- Use the ETL_Datetime consistently for datetime references within this ETL process.
Where ad.Active_Flag = 1 ; 			-- For an Course, only expire the curretn Active record.






/****************************  PERFORM INSERTS AND UP-SERTS  **********************************/

/*
This step inserts NEW fact ID records and inserts new Active records for existing IDs ( "Up-Sert" rather than an "Update" )

*/
 

-- Perform Insert for any new Courses.
INSERT INTO Answer_Fact 
	(   

	Framestate_ID,
	Deleted_Flag,
	Deleted_Datetime,
	Effective_Datetime,
	Expiration_Datetime,
	Active_Flag,
    
	Account_ID,
	Trainee_ID,
	Course_ID,
	Module_ID,
	Quiz_ID,
	Question_ID,
	Status_ID,
    
	First_Added_Date,
	First_Added_Time,
	First_Added_Datetime,
	Last_Modified_Date,
	Last_Modified_Time,
	Last_Modified_Datetime,
	Answer_Value,
	Correct_Answer_Flag,
	Answer_Index_Selected,
    Essay_Answer_Length,
	ETL_Run_Datetime

	 )
     
 

SELECT	Distinct		
cc.framestateid, 
Case When Deleted is not null Then 1 Else 0 End as Deleted_Flag,
ifnull(cc.deleted,'1900-01-01') as Deleted_Datetime,


ETL_Datetime as Effective_Datetime,
null as Expiration_Datetime,
1 as Active_Flag,

cc.accountid as Account_ID,
cc.userid as Trainee_ID,
cc.courseid as Course_ID,
cc.cmoduleid as Module_ID,
cc.quizid as Quiz_ID,
cc.questionid as Question_ID,
cc.status as Status_ID,

cast(cc.created as date) as First_Added_Date,
cast(cc.created as time) as First_Added_Time,
cc.created as First_Added_Datetime,

cast(cc.modified as date) as  Last_Modified_By_Date,
cast(cc.modified as time) as Last_Modified_By_Time,
cc.modified as Last_Modified_By_Datetime,
   
    
-- Answer Value
		-- case statements, depending on question type, parse scoproperties correctly for the answer.
Case 
	 -- True / False Cases
	 When  substring(substring(cc.scoproperties, 100,50 ) ,  instr(substring(cc.scoproperties, 100,50 ),"Answer") + 8 , 4)  = "true" then "True"
	 When  substring(substring(cc.scoproperties, 100,50 ) ,  instr(substring(cc.scoproperties, 100,50 ),"Answer") + 8 , 4)  = "fals" then "False"
	 Else "N/A"
End as Answer_Value,
    

 
cc.QuestionIsCorrect as Correct_Answer_Flag,    -- Sourced directly from the framestate record. No rule required.
 
cc.answerindexselected as Answer_Index_Selected ,

cc.essayanswerlength as Essay_Answer_Length,
 
 
ETL_Datetime as ETL_Run_Datetime

From cc_FrameState cc


left outer join Existing_Dim_IDs ED  -- Only insert records for Course IDs that do not previously exist in Answer_Fact_Fact.
on cc.FrameStateID = ED.ID

left outer join TChange_Log_Ref clf
on cc.FrameStateID = clf.Source_Record_ID

left outer join Answer_Dim ad		-- get only the correct answer record 
on cc.questionid = ad.question_id
and ad.active_flag = 1
and correct_answer_flag = 1

Where  cc.frametype in (  108000300 , 108000800  )  -- Questions Records Only from the FrameState extract.  Confirmed.          SurveyQuestionFrame	108000800

-- New Records.
And ED.ID is Null

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
Inner Join Answer_Fact d 				
On r.table_name = "Answer_Fact"
And r.source_record_id = d.FrameState_id
And d.active_flag = 1
Set r.Record_ID = d.AF_ID    				-- This is the auto_increment record id.
;




/****************  CLEAN UP ANY TEMPORARY TABLES ***************/
Drop Table If Exists Existing_Dim_IDs;
Drop Table If Exists TChange_Log ; 
Drop Table If Exists TChange_Log_Ref_2 ;




COMMIT;

END$$
DELIMITER ;



-- End of script
