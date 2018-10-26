use pacifica ; 




-- TEST 1 - COMPLETE LOAD

/*** perform a full load of the Quiz table into Quiz_Dim ***/

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "Quiz"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime, status)
select "Quiz", cast("1900-01-21 00:00:00" as datetime), 0   ;

Select * from Control_Change_Capture ;


select count(*) from Quiz ;

-- call the stored procedure to extract all since the control date.   Note that this will re-set the control date to a new current value.  

-- call the stored procedure
CALL Quiz_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;



-- confirm
select count(*) from cc_Quiz ; 

select * from cc_Quiz ;


-- Remove all
Truncate Table Quiz_Dim ; 

 -- call the stored procedure
CALL Quiz_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;

   



-- Confirm Count
select count(*) from Quiz_Dim; 


-- Check Change Log
select * from Change_Log ; 



-- TEST 2, CHANGE SOME RECORDS IN THE CONTROL CAPTURE TABLE TO SEE IF THEY CREATE NEW RECORDS IN THE DIMENSION
 
-- Use the existing cc_Quiz data above, but change some of the values.
update cc_Quiz
set name = "test name"
where name < "r" ; 

 




-- call the stored procedure
CALL Quiz_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;


-- Confirm Count
select count(*) from Quiz_Dim; 


select * from Quiz_dim order by Quiz_id, active_flag desc ; 

-- Check Change Log
select data_change_message, table_name, effective_datetime, max(id), count(*) from Change_Log  group by data_change_message, table_name, effective_datetime ;

select count(*) from change_Log ;
select * from change_Log
Select distinct source_record_id from change_log




-- View Change Report on Module_Dim data.    THIS QUERY PRODUCES DUPLICATES IN TESTING BECAUSE THE EFFECTIVE-DATETIME DOES NOT CHANGE, WHILE IN PRACTICE IT WOULD.
select c.data_change_message, a.*
from Quiz_dim a
left outer join change_log c
on c.record_id = a.id
and c.effective_datetime = a.etl_run_datetime
order by a.Quiz_id, a.active_flag desc ;








select * from Quiz_dim; 

select * from Quiz_dim where active_flag = 1; 

select * from cc_Quiz ; 

select * from Existing_Dim_IDs ; 

truncate table Quiz_dim ;


select * from Quiz_dim where Quiz_name < "h"  -- 747
delete from Quiz_dim where Quiz_name < "h"  ;


update cc_Quiz
set description = "test 4"
where name < "r"

update Quiz_dim
set Quiz_description = "old description "
where Quiz_name < "r"


select count(*) from cc_Quiz where description = "test" 
select count(*) from Quiz_dim where Quiz_Description = "test" 


select * from change_log_ref

-- check
select clf.Data_Change_Message, clf.Effective_Datetime, ad.* 
from change_log_ref  clf
inner join Quiz_dim ad
on clf.record_id = ad.Quiz_id
and ad.effective_datetime = clf.Effective_Datetime
order by ad.Quiz_id, active_flag desc



select * from change_log where Quiz_Description_Changed = 1 ; 

select Quiz_description , description, name
from Quiz_dim ad
inner join cc_Quiz cc
on ad.Quiz_id = cc.id
where Quiz_description <> description


update cc_Quiz set description = "test"

update Quiz_dim set Quiz_description = "not test"




select * from Existing_Dim_IDs ; 



select * from change_log_ref


Select * from cc_Quiz where description = "test"

2012-06-13 06:59:08


Select * from Quiz_dim where Quiz_id = 18380597

2012-06-13 06:59:08

select 1076 - 747
