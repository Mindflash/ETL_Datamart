use pacifica ; 




select count(*) from answer_Fact ; 
1098530


select count(*) 
from (

	select distinct course_id, question_id,  trainee_id from answer_Fact   where active_flag = 1

	) a ;


1098530  course_id, question_id,  trainee_id
1098530   course_id, question_id, trainee_id
1098530   course_id, question_id, module_id , trainee_id
1075893   question_id, module_id , trainee_id 
250347    question_id, module_id




select count(*) from quiz_participation_fact ; 














/*** perform a full load of the Trainee_Certificate table into Trainee_Certificate_Dim ***/

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "Framestate"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime )
select "Framestate", cast("1900-01-21 00:00:00" as datetime)    ;

Select * from Control_Change_Capture ;




-- call the stored procedure to extract all since the control date.   Note that this will re-set the control date to a new current value.  

-- call the stored procedure
CALL Framestate_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;



-- confirm
select count(*) from CC_Framestate ;

select * from CC_Framestate ;


-- Remove all
Truncate Table Quiz_Participation_Fact ; 

Truncate Table change_log;


-- call the stored procedure
CALL Quiz_Participation_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;

   
-- Confirm Count
select count(*) from Quiz_Participation_Fact ;

select * from Quiz_Participation_Fact ;


Select * 

From cc_FrameState cc
Where  cc.frametype in (108000500 , 108000400)
order by frameid, framestateid






-- Check Change Log
select * from Change_Log ; 


select * from cc_QuizFact ;


-- TEST 2, CHANGE SOME RECORDS IN THE CONTROL CAPTURE TABLE TO SEE IF THEY CREATE NEW RECORDS IN THE DIMENSION
 
-- Use the existing cc_Trainee_Certificate data above, but change some of the values.
update cc_QuizFact
set cmodulecompleted = 0
where   substring(cmodulecompleted,1,4) < 2015


select * from cc_QuizFact


select substring(invitedbyuserId,1,2)
from  cc_QuizFact
where  substring(invitedbyuserId,1,2) < 14

select * from  Trainee_Certificate_Fact
select description from cc_Trainee_Certificate where name < "r"
select Trainee_Certificate_description from Trainee_Certificate_dim where Trainee_Certificate_name < "r"




-- call the stored procedure
CALL Trainee_Certificate_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;


-- Confirm Count
select count(*) from Trainee_Certificate_Fact; 





select * from Trainee_Certificate_fact order by QuizFact_id, active_flag desc ; 

-- Check Change Log
select data_change_message, table_name, effective_datetime, max(id), count(*) from Change_Log  group by data_change_message, table_name, effective_datetime ;

select count(*) from change_Log ;
select * from change_Log
Select distinct source_record_id from change_log




-- View Change Report on Module_Dim data.    THIS QUERY PRODUCES DUPLICATES IN TESTING BECAUSE THE EFFECTIVE-DATETIME DOES NOT CHANGE, WHILE IN PRACTICE IT WOULD.
select distinct c.data_change_message, a.*
from Trainee_Certificate_fact a
left outer join change_log c
on c.record_id = a.tcf_id
and c.effective_datetime = a.etl_run_datetime
order by a.QuizFact_id, a.active_flag desc ;








select * from Trainee_Certificate_dim; 

select * from Trainee_Certificate_dim where active_flag = 1; 

select * from cc_Trainee_Certificate ; 

select * from Existing_Dim_IDs ; 

truncate table Trainee_Certificate_dim ;


select * from Trainee_Certificate_dim where Trainee_Certificate_name < "h"  -- 747
delete from Trainee_Certificate_dim where Trainee_Certificate_name < "h"  ;


update cc_Trainee_Certificate
set description = "test 4"
where name < "r"

update Trainee_Certificate_dim
set Trainee_Certificate_description = "old description "
where Trainee_Certificate_name < "r"


select count(*) from cc_Trainee_Certificate where description = "test" 
select count(*) from Trainee_Certificate_dim where Trainee_Certificate_Description = "test" 


select * from change_log_ref

-- check
select clf.Data_Change_Message, clf.Effective_Datetime, ad.* 
from change_log_ref  clf
inner join Trainee_Certificate_dim ad
on clf.record_id = ad.Trainee_Certificate_id
and ad.effective_datetime = clf.Effective_Datetime
order by ad.Trainee_Certificate_id, active_flag desc



select * from change_log where Trainee_Certificate_Description_Changed = 1 ; 

select Trainee_Certificate_description , description, name
from Trainee_Certificate_dim ad
inner join cc_Trainee_Certificate cc
on ad.Trainee_Certificate_id = cc.id
where Trainee_Certificate_description <> description


update cc_Trainee_Certificate set description = "test"

update Trainee_Certificate_dim set Trainee_Certificate_description = "not test"




select * from Existing_Dim_IDs ; 



select * from change_log_ref


Select * from cc_Trainee_Certificate where description = "test"

2012-06-13 06:59:08


Select * from Trainee_Certificate_dim where Trainee_Certificate_id = 18380597

2012-06-13 06:59:08

select 1076 - 747
