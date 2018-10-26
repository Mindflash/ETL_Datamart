use pacifica ; 




-- TEST 1 - COMPLETE LOAD

/*** perform a full load of the Answer table into Answer_Dim ***/

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "Answer"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime  )
select "Answer", cast("1900-01-21 00:00:00" as datetime)    ;

Select * from Control_Change_Capture ;


select count(*) from Answer ;

-- call the stored procedure to extract all since the control date.   Note that this will re-set the control date to a new current value.  

-- call the stored procedure
CALL Answer_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;



-- confirm
select count(*) from cc_Answer ; 


select * from cc_Answer ;


-- Remove all
Truncate Table Answer_Dim ; 

 -- call the stored procedure
CALL Answer_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;

   



-- Confirm Count
select * from Answer_Dim; 


-- Check Change Log
select * from Change_Log ; 



-- TEST 2, CHANGE SOME RECORDS IN THE CONTROL CAPTURE TABLE TO SEE IF THEY CREATE NEW RECORDS IN THE DIMENSION
 
-- Use the existing cc_Answer data above, but change some of the values.
update cc_Answer
set title = "test title"
where title < "r" ; 

 

-- call the stored procedure
CALL Answer_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;


-- Confirm Count
select count(*) from Answer_Dim; 


select * from Answer_dim order by Answer_id, active_flag desc ; 

-- Check Change Log
select data_change_message, table_name, effective_datetime, max(id), count(*) from Change_Log  group by data_change_message, table_name, effective_datetime ;

select count(*) from change_Log ;
select * from change_Log
Select distinct source_record_id from change_log




-- View Change Report
select c.data_change_message, a.*
from Answer_dim a
left outer join change_log c
on c.record_id = a.id
and c.effective_datetime = a.etl_run_datetime
order by a.Answer_id, a.active_flag desc ;








select * from Answer_dim; 

select * from Answer_dim where active_flag = 1; 

select * from cc_Answer ; 

select * from Existing_Dim_IDs ; 

truncate table Answer_dim ;


select * from Answer_dim where Answer_name < "h"  -- 747
delete from Answer_dim where Answer_name < "h"  ;


update cc_Answer
set description = "test 4"
where name < "r"

update Answer_dim
set Answer_description = "old description "
where Answer_name < "r"


select count(*) from cc_Answer where description = "test" 
select count(*) from Answer_dim where Answer_Description = "test" 


select * from change_log_ref

-- check
select clf.Data_Change_Message, clf.Effective_Datetime, ad.* 
from change_log_ref  clf
inner join Answer_dim ad
on clf.record_id = ad.Answer_id
and ad.effective_datetime = clf.Effective_Datetime
order by ad.Answer_id, active_flag desc



select * from change_log where Answer_Description_Changed = 1 ; 

select Answer_description , description, name
from Answer_dim ad
inner join cc_Answer cc
on ad.Answer_id = cc.id
where Answer_description <> description


update cc_Answer set description = "test"

update Answer_dim set Answer_description = "not test"




select * from Existing_Dim_IDs ; 



select * from change_log_ref


Select * from cc_Answer where description = "test"

2012-06-13 06:59:08


Select * from Answer_dim where Answer_id = 18380597

2012-06-13 06:59:08

select 1076 - 747
