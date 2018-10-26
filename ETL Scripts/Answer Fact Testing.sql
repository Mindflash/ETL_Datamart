use pacifica ; 




-- TEST 1 - COMPLETE LOAD

/*** perform a full load of the AnswerFact table into Answer_Fact ***/

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "Framestate"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime  )
select "Framestate", cast("1900-01-21 00:00:00" as datetime)    ;

Select * from Control_Change_Capture ;

truncate table cc_Framestate ; 


-- call the stored procedure to extract all since the control date.   Note that this will re-set the control date to a new current value.  

-- call the stored procedure
CALL Framestate_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;



-- confirm 
select * from cc_framestate where questiontype like "%essay%" ; 


select distinct frametype from cc_framestate where questiontype like "%essay%" ; 

-- Remove all
Truncate Table Answer_Fact ; 

 -- call the stored procedure
CALL Answer_Fact_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;

create index ndx_af_01 on Answer_Fact (question_id) ; 

create index ndx_qd_01 on Question_Dim (question_id) ; 
   
select count(distinct question_id) from question_dim ; 

select count(*) from question_dim 

select * from question_dim ; 


select af.*
from answer_fact af
inner join question_dim qd
on af.question_id = qd.question_id

where question_type = "essay"
order by af.question_id

limit 1000 ; 



-- Confirm Count
select * from Answer_Fact where essay_answer_length > 0; 

select count(*) from Answer_Fact ; 

-- Check Change Log
select * from Change_Log ; 

