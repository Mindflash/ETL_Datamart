use pacifica; 

DROP PROCEDURE IF EXISTS Answer_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Answer_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN


/********** BEGIN OF STORED PROCEDURE ***********/

/*
Source is cc_Answer
Target is Answer_Dim

Using an Effective_Datetime from the Change Capture Etl process to check for multiple "batch" loads.  This date is loaded into the Dim table when records are inserted or changed.
It serves as the Batch Run identifier and is compared to ensure duplicates are not inserted if the load was called twice in the same <time period>.  The time period can be 
adjusted.   Once the date changes it would allow the load to perform again.  This is an unlikely corner case, but could be improved if needed.

Approach for Slowly Changing Dimension is to Insert new records, and for changed records, 1. update the existing active 
record by changing it to INActive, and 2, inserting (UpSert) a new Active Record.   We never Update data (other than the SCD fields) on a record.

So the process steps are 
1. Identify cc_Answer records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.


*/



/*    DDL For Dimension

Goal: to load all answers to questions, and identify those answers which are correct choices.alter



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
	Set ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_Answer ); 
    



/*************** Prepare some working tables ******************/


-- Get all of the existing Answer_ID values from Answer_Dim with the Extract Datetime.  We treat Effective_Datetime as a logical Batch ID.
Drop Table If Exists Existing_Dim_IDs;
Create temporary table Existing_Dim_IDs (ID bigint, Effective_Datetime datetime);
Insert Into Existing_Dim_IDs
Select distinct Answer_ID, Effective_Datetime from Answer_Dim where Active_Flag = 1; 


--      select * from cc_answer order by questionid ;         select * from answer_dim



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
cc.modified as Last_Modified_Datetime,
cc.answerid as Answer_ID,
Case When cc.deleted <> dim.deleted_datetime Then 1 Else 0 End as Deleted_Date_Changed,
Case When cc.AccountID <> dim.Account_ID Then 1 Else 0 End as Account_ID_Changed,

Case When cc.answervalue <> dim.Answer_Value Then 1 Else 0 End as Answer_Value_Changed,
Case When cc.correctanswerflag <> dim.Correct_Answer_Flag Then 1 Else 0 End as Correct_Answer_Flag_Changed,
Case When cc.combinationanswer <> dim.Combination_Answer_Description Then 1 Else 0 End as Combination_Answer_Changed,
Case When cc.sequenceanswer <> dim.Sequence_Answer_Index Then 1 Else 0 End as Sequence_Answer_Flag_Changed

From cc_Answer cc

Inner Join Answer_dim dim
On cc.answerid = dim.Answer_ID

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
"Answer" ,
Answer_ID, 
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
"Answer" ,
Answer_ID, 
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
"Answer" ,
Answer_ID, 
"Answer_Value_Changed"
From TChange_Log 
Where Answer_Value_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Answer" ,
Answer_ID, 
"Correct_Answer_Flag_Changed"
From TChange_Log 
Where Correct_Answer_Flag_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Answer" ,
Answer_ID, 
"Combination_Answer_Changed"
From TChange_Log 
Where Combination_Answer_Changed = 1 ; 


Insert Into TChange_Log_Ref
(Effective_Datetime ,
Last_Modified_Datetime, 
Table_Name  ,
Source_Record_ID ,
Data_Change_Message )
Select
ETL_Datetime, 
Last_Modified_Datetime,
"Answer" ,
Answer_ID, 
"Sequence_Answer_Flag_Changed"
From TChange_Log 
Where Sequence_Answer_Flag_Changed = 1 ; 




/********************** PERFORM SLOWLY CHANGING DIMENSION UPDATES TO DIM RECORDS ******************/

/*
The process steps are 
1. Identify cc_Answer records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.
*/


-- These are your distinct IDs that are to be Up-Serted.   select distinct Source_Record_ID from TChange_Log_Ref


-- Expire old records that have changed.
UPDATE	Answer_Dim ad
Inner Join TChange_Log_Ref cl 		-- these are your change capture records that are updates.
On cl.Source_Record_ID = ad.Answer_ID
Set ad.Active_Flag = 0,
ad.Expiration_Datetime = ETL_Datetime 	-- Use the ETL_Datetime consistently for datetime references within this ETL process.
Where ad.Active_Flag = 1 ; 			-- For an Answer, only expire the curretn Active record.







/****************************  PERFORM INSERTS AND UP-SERTS  **********************************/

/*
This step inserts NEW dimension ID records and inserts new Active records for existing IDs ( "Up-Sert" rather than an "Update" )

truncate table answer_dim;

select * from cc_answer order by questionid, indexid ; 

*/


-- Perform Insert for any new Answers for     True False ,  Mulitple Correct , Multiple Choice,  Answer Sequence, Image Caption, Image Parts
	INSERT INTO Answer_dim
	(Effective_Datetime,
	Expiration_Datetime,
	Active_Flag,
	Deleted_Flag,
	Deleted_Datetime,
	Question_ID,
	Answer_ID,
	Account_ID,
	First_Added_Datetime,
	Last_Modified_Datetime,
	Answer_Value,
    Correct_Answer_Flag,
    Combination_Answer_Description,
    Sequence_Answer_Index,
	Quiz_ID,
	Answer_Index_Number,
	Question_Type,
	Process_Flag,

	ETL_Run_Datetime
	)

	SELECT	Distinct		-- This is the Mapping between cc_Answer and Answer_dim, with transformations ... mostly lookups against mtype references.

	ETL_Datetime as Effective_Datetime,
	null as Expiration_Datetime,
	1 as Active_Flag,
	Case When Deleted is not null Then 1 Else 0 End as Deleted_Flag,
	cc.deleted as Deleted_Date,
	cc.questionid as Question_ID,
	cc.answerid  as Answer_ID,
	cc.accountid as Account_ID,
	cc.Created as First_Added_Date,
	cc.Modified as Last_Modified_Date,
	cc.answervalue as Answer_Value,
    cc.correctanswerflag as Correct_Answer_Flag,
    cc.combinationanswer as Combination_Answer_Description,
    cc.sequenceanswer as Sequence_Answer_Index,
	cc.quizid as Quiz_ID,
	cc.indexid as Answer_Index_Number,
	cc.questiontype as Question_Type,
	1 as Process_Flag,

	ETL_Datetime

	From cc_Answer cc

	left outer join Existing_Dim_IDs ED  -- Only insert records for Answer IDs that do not previously exist in Answer_Dim.
	on cc.AnswerID = ED.ID

	left outer join TChange_Log_Ref clf
	on cc.AnswerID = clf.Source_Record_ID

	-- You want to perform Inserts for all new cc_Answer records, all changed existing records, but not existing records in cc_Answer that did not change.
	Where 

	-- New Records.
	ED.ID is Null

	OR

	-- Changed Records.
	clf.Source_Record_ID is not null

	;



	 -- This inserts the Incorrect T/F Answers for those that were added or Updated
	INSERT INTO `pacifica`.`answer_dim`
	( 
	`Effective_Datetime`,
	`Expiration_Datetime`,
	`Active_Flag`,
	`Deleted_Flag`,
	`Deleted_Datetime`,
	`Question_ID`,
	`Answer_ID`,
	`Account_ID`,
	`First_Added_Datetime`,
	`Last_Modified_Datetime`,
	`Correct_Answer_Flag`,    
 	`Combination_Answer_Description`,
	`Sequence_Answer_Index`,
	`Answer_Value`,
	`Quiz_ID`,
	`Answer_Index_Number`,
	`Question_Type`,
	`Process_Flag`,
	`ETL_Run_Datetime`)

	SELECT
	`Effective_Datetime`,
	`Expiration_Datetime`,
	`Active_Flag`,
	`Deleted_Flag`,
	`Deleted_Datetime`,
	`Question_ID`,
	 Answer_ID + 1 ,
	`Account_ID`,
	`First_Added_Datetime`,
	`Last_Modified_Datetime`,
	 0,  				-- as correct answer flag
	"N/A" as CombinationAnswer,
    "N/A" as SequenceAnswer,
	 Case When Answer_Value = "True" then "False" Else "True" End as Answer_Value,
	`Quiz_ID`,
	`Answer_Index_Number`,
	`Question_Type`,
	`Process_Flag`,
	`ETL_Run_Datetime`
	From Answer_Dim 
	Where Question_Type = 'TF'
	And Process_Flag = 1 ; 


	-- ReSet Process_Flag 
	Update Answer_Dim
	Set Process_Flag = 0
	Where Process_Flag = 1 ; 

	-- End of TrueFalse Up-Serts
    



/****************  UPDATE Data_Change_Ref ***************/
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
Inner Join Answer_Dim d 				
On r.table_name = "Answer"
And r.source_record_id = d.Answer_id
And d.active_flag = 1
Set r.Record_ID = d.ID    				-- This is the auto_increment record id.
;





/****************  CLEAN UP ANY TEMPORARY TABLES ***************/

Drop Table If Exists Existing_Dim_IDs;
Drop Table If Exists TChange_Log ; 
Drop Table If Exists TChange_Log_Ref_2 ;



-- Create Index
	Drop Index ndx_AnswerDim_01  On Answer_Dim;

	Create Index ndx_AnswerDim_01 on Answer_Dim (question_id, active_flag, correct_answer_flag) ;




COMMIT;

END$$
DELIMITER ;



-- End of script

