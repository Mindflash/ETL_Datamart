


use pacifica; 


/*****   For DEV I have set limits of 1000 records on all of the Change Capture ETL procs  **********/


/*
Process uses a control table that contains the last successful extract time stamp, on the table's MODIFIED datetime.
These values are over-written each run, so there is one record per ETL table.   Error Handling in general uses 0 for success, 1 for failure.

		Select * from Control_Change_Capture ;
        
Using an Effective_Datetime from the Change Capture Etl process to check for multiple "batch" loads.  This date is loaded into the Dim table when records are inserted or changed.
It serves as the Batch Run identifier and is compared to ensure duplicates are not inserted if the load was called twice in the same <time period>.  The time period can be 
adjusted.   Once the date changes it would allow the load to perform again.  This is an unlikely corner case, but could be improved if needed.

Approach for Slowly Changing Dimension is to INsert new records, and for changed records, 1. update the existing active 
record by changing it to INActive, and 2, inserting (UpSert) a new Active Record.   We never Update data (other than the SCD fields) on a record.

So the process steps are 
1. Identify cc_account records that are Updates.  Log them.
2. Perform SCD field updates on those records.
3. Insert BOTH all the new records for IDs that never existed and the new Active records for IDs that did exist.

        

*/




-- Here is the test settings for the control dates.
truncate table Control_Change_Capture ;
    
    
    
Select * from Control_Change_Capture ;


-- Change Capture for ACCOUNT
delete from control_change_Capture  where table_name = "account"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime)
select "Account", cast("1900-01-21 00:00:00" as datetime)   ;


-- Change Capture for USERRECORD
delete from control_change_Capture  where table_name = "UserRecord"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime)
select "UserRecord", cast("1900-01-21 00:00:00" as datetime)   ;


-- Change Capture for USERGroup
delete from control_change_Capture  where table_name = "UserGroup"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime)
select "UserGroup", cast("1900-01-21 00:00:00" as datetime)   ;


-- Change Capture for CModule
delete from control_change_Capture  where table_name = "CModule"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime)
select "CModule", cast("1900-01-21 00:00:00" as datetime)   ;


-- Change Capture for Course
delete from control_change_Capture  where table_name = "Course"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime)
select "Course", cast("1900-01-21 00:00:00" as datetime)   ;


-- Change Capture for Question
delete from control_change_Capture  where table_name = "Question"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime)
select "Question", cast("1900-01-21 00:00:00" as datetime)   ;


-- Change Capture for Quiz
delete from control_change_Capture  where table_name = "Quiz"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime)
select "Quiz", cast("1900-01-21 00:00:00" as datetime)   ;


-- Change Capture for coursestate
delete from control_change_Capture  where table_name = "CourseState"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime)
select "CourseState", cast("1900-01-21 00:00:00" as datetime)   ;


-- Change Capture for FrameState
delete from control_change_Capture  where table_name = "FrameState"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime)
select "FrameState", cast("1900-01-21 00:00:00" as datetime)   ;

 


	-- see that the control dates have been updated.
	Select * from Control_Change_Capture ;






/*
Work Flow

1. run daily change capture etl to create new change data tables 

2. run daily star schema load etl 

3. if successful, run archive of daily capture data to archive DB and then truncate the daily change capture tables.alter

	If not successful, stop processing so that you have access to the daily change capture data for recovery.
    
    
4. run daily metrics on change_log activity and record to a summary table for reporting.

5. ... then likely will proceed with any aggregation / summary processes.

*/



-- Change Capture procedures With Control Dates.  Incrememntal Loads.
	CALL Account_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL UserRecord_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL UserGroup_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL CModule_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Course_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Question_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Quiz_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Answer_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
    
    
-- Fact Change Capture   
	CALL CourseState_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL CModuleState_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Framestate_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;          -- used as source for both Quiz_Participation_Fact   and   Answer_Fact
	CALL TraineeCertificate_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;

    
-- Change Capture WithOut Control Dates.  These are Drop / Refresh.
	CALL MType_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Course_Grade_Status_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Product_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Status_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Tier_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
    
    
    
    select * from cc_framestate ;
        
    select count(*)  from cc_framestate ;
	select distinct frametypedesc from cc_framestate ;
    



-- SP Controller
    
DROP PROCEDURE IF EXISTS Change_Capture_ETL;

DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Change_Capture_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150), OUT SubCallName varchar(50))
BEGIN

    Set SP_Result = 1;

	Changecapture: BEGIN
    
    
		-- Account
		CALL Account_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "Account_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;
    
		-- UserRecord
		CALL UserRecord_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "UserRecord_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;
        
        
		-- UserGroup
		CALL UserGroup_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "UserGroup_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;        
        

		-- CModule
		CALL CModule_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "CModule_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;    
        
        
		-- Course
		CALL Course_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "Course_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;   
        
        
		-- Question
		CALL Question_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "Question_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;   
        
        
		-- Quiz
		CALL Quiz_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "Quiz_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;   
        
 
 		-- Answer
		CALL Answer_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "Answer_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;  
        
 
 		-- FrameState  source for both Answer_Fact and Quiz_Participation_Fact 
		CALL Framestate_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "Framestate_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;  
        
        
    
		-- Trainee Certificate
		CALL TraineeCertificate_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "TraineeCertificate_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;  
 
 
 
  		-- Course State
		CALL CourseState_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "CourseState_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;  
        
        
  
 		-- CModule State
		CALL CModuleState_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "CModuleState_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF; 
        
        
 		-- MType
		CALL MType_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "MType_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;  
        

		-- Course_Grade_Status
		CALL Course_Grade_Status_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "Course_Grade_Status_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;  
        

		-- Product
		CALL Product_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "Product_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;          
        
        
 		-- Status
		CALL Status_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "Status_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;         
        
 		-- Tier
		CALL Tier_ChgCap_ETL (@STATE, @MESSAGE);  
		IF @State = 1 Then 
			Begin
                Set SubCallName = "Tier_ChgCap_ETL";
                Set SP_Message = @Message;
				LEAVE Changecapture ;
            End;
		End IF;  
        

        
		-- Set Success at end
		Set SP_Result = 0;
    
    End Changecapture ;


END$$
DELIMITER ;
    
    
    
    
    
    
    
Call Change_Capture_ETL (@STATE, @MESSAGE,@SUBCALLNAME);    Select @State, @Message, @SubCallName ;


-- Check to see if it ran.
Select now(), c.* from Control_Change_Capture c ;
    
    
    
    
    
    
	-- see that the control dates have been updated.
	Select * from Control_Change_Capture ;




-- Change_Log

	Select * from Change_Log ;

	-- Can join to a dimension to see the list of data change messages for each set of dimension records (ie., account_id values are grouped)
	Select c.data_Change_Message, a.*
    from course_dim a
    inner join change_log c
    on a.ID = c.record_id 
    and c.table_name = "Course"
    order by course_ID, active_flag desc ;







	-- see that the control dates have been updated.
	Select * from Control_Change_Capture ;




-- Other Dimensions that are Script Populated

	-- Date_Dim is loaded from a script
	Select * from date_dim; 


	-- Time_Dim is loaded from a script
    Select * from time_dim; 



select * from module_dim;




-- Testing

	-- truncate all dim / fact tables
	truncate table account_dim;
	truncate table user_dim;
	truncate table user_group_dim;
	truncate table quiz_dim;
	truncate table question_dim;
	truncate table course_dim;
	truncate table module_dim;
	truncate table product_dim;

	truncate table mtype_ref;
	truncate table tier_ref;
	truncate table status_dim;
	truncate table course_grade_status_dim;
	truncate table module_grade_status_dim;

	truncate table course_participation_fact;
    truncate table module_participation_fact;
    truncate table traineecertificate_fact;
	truncate table quiz_participation_fact;
 	truncate table Answer_fact;   



-- Run Call Star Schema  ETL

	CALL Account_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL User_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL UserGroup_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Module_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Course_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Question_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Quiz_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL MType_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Course_Grade_Status_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Module_Grade_Status_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Product_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Status_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Tier_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;

 	CALL Answer_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
    
	CALL Course_Participation_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;
	CALL Quiz_Participation_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;    
	CALL Answer_Fact_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;     
    
    
    

	-- confirm
	select * from  account_dim;
	select * from  user_dim;
	select * from  user_group_dim;
	select * from  quiz_dim;
	select * from  question_dim;
	select * from  course_dim;
	select * from  module_dim;
	select * from  product_dim;

	select * from  mtype_ref;
	select * from  tier_ref;
	select * from  status_dim;
	select * from  course_grade_status_dim;
	select * from  module_grade_status_dim;
    
    select * from course_participation_fact;
    select * from quiz_participation_fact;
    select * from answer_Fact ; 



  select * from answer_Fact where essay_answer_length > 0 ; 
  
  select count(*) from quiz_participation_fact;
    
    
    