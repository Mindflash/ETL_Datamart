use pacifica ; 




-- TEST 1 - COMPLETE LOAD

/*** perform a full load of the Question table into Question_Dim ***/

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "Question"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime, status)
select "Question", cast("1900-01-21 00:00:00" as datetime), 0   ;

Select * from Control_Change_Capture ;


select count(*) from Question ;

-- call the stored procedure to extract all since the control date.   Note that this will re-set the control date to a new current value.  

-- call the stored procedure
CALL Question_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;



-- confirm
select count(*) from cc_Question ; 

select * from cc_Question ;


-- Remove all
Truncate Table Question_Dim ; 

 -- call the stored procedure
CALL Question_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;

   



-- Confirm Count
select count(*) from Question_Dim; 


-- Check Change Log
select * from Change_Log ; 



-- TEST 2, CHANGE SOME RECORDS IN THE CONTROL CAPTURE TABLE TO SEE IF THEY CREATE NEW RECORDS IN THE DIMENSION
 
-- Use the existing cc_Question data above, but change some of the values.
update cc_Question
set title = "test title"
where title < "r" ; 

 

-- call the stored procedure
CALL Question_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;


-- Confirm Count
select count(*) from Question_Dim; 


select * from Question_dim order by Question_id, active_flag desc ; 

-- Check Change Log
select data_change_message, table_name, effective_datetime, max(id), count(*) from Change_Log  group by data_change_message, table_name, effective_datetime ;

select count(*) from change_Log ;
select * from change_Log
Select distinct source_record_id from change_log




-- View Change Report
select c.data_change_message, a.*
from Question_dim a
left outer join change_log c
on c.record_id = a.id
and c.effective_datetime = a.etl_run_datetime
order by a.Question_id, a.active_flag desc ;








select * from Question_dim; 

select * from Question_dim where active_flag = 1; 

select * from cc_Question ; 

select * from Existing_Dim_IDs ; 

truncate table Question_dim ;


select * from Question_dim where Question_name < "h"  -- 747
delete from Question_dim where Question_name < "h"  ;


update cc_Question
set description = "test 4"
where name < "r"

update Question_dim
set Question_description = "old description "
where Question_name < "r"


select count(*) from cc_Question where description = "test" 
select count(*) from Question_dim where Question_Description = "test" 


select * from change_log_ref

-- check
select clf.Data_Change_Message, clf.Effective_Datetime, ad.* 
from change_log_ref  clf
inner join Question_dim ad
on clf.record_id = ad.Question_id
and ad.effective_datetime = clf.Effective_Datetime
order by ad.Question_id, active_flag desc



select * from change_log where Question_Description_Changed = 1 ; 

select Question_description , description, name
from Question_dim ad
inner join cc_Question cc
on ad.Question_id = cc.id
where Question_description <> description


update cc_Question set description = "test"

update Question_dim set Question_description = "not test"




select * from Existing_Dim_IDs ; 



select * from change_log_ref


Select * from cc_Question where description = "test"

2012-06-13 06:59:08


Select * from Question_dim where Question_id = 18380597

2012-06-13 06:59:08

select 1076 - 747
