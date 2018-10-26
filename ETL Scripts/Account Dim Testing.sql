use pacifica ; 




-- TEST 1 - COMPLETE LOAD

/*** perform a full load of the account table into Account_Dim ***/

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "account"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime, status)
select "Account", cast("1900-01-21 00:00:00" as datetime), 0   ;

Select * from Control_Change_Capture ;


select count(*) from account ;

-- call the stored procedure to extract all since the control date.   Note that this will re-set the control date to a new current value.  

-- call the stored procedure
CALL Account_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;



-- confirm
select count(*) from cc_account ; 

select * from cc_account ;


-- Remove all
Truncate Table Account_Dim ; 

Truncate Table change_log;


-- call the stored procedure
CALL Account_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;

   



-- Confirm Count
select count(*) from Account_Dim; 


-- Check Change Log
select * from Change_Log ; 



-- TEST 2, CHANGE SOME RECORDS IN THE CONTROL CAPTURE TABLE TO SEE IF THEY CREATE NEW RECORDS IN THE DIMENSION
 
-- Use the existing cc_account data above, but change some of the values.
update cc_account
set status = 127000500
where status  = 127000100 ; 

update cc_account
set owneremail = "test 1"
where owneremail  < "k" ;



select * from cc_account



select description from cc_account where name < "r"

select account_description from account_dim where account_name < "r"




-- call the stored procedure
CALL Account_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;


-- Confirm Count
select count(*) from Account_Dim; 


select * from account_dim order by account_id, active_flag desc ; 

-- Check Change Log
select data_change_message, table_name, effective_datetime, max(id), count(*) from Change_Log  group by data_change_message, table_name, effective_datetime ;

select count(*) from change_Log ;
select * from change_Log
Select distinct source_record_id from change_log




-- View Change Report on Module_Dim data.    THIS QUERY PRODUCES DUPLICATES IN TESTING BECAUSE THE EFFECTIVE-DATETIME DOES NOT CHANGE, WHILE IN PRACTICE IT WOULD.
select c.data_change_message, a.*
from account_dim a
left outer join change_log c
on c.record_id = a.id
and c.effective_datetime = a.etl_run_datetime
order by a.account_id, a.active_flag desc ;








select * from account_dim; 

select * from account_dim where active_flag = 1; 

select * from cc_account ; 

select * from Existing_Dim_IDs ; 

truncate table account_dim ;


select * from account_dim where account_name < "h"  -- 747
delete from account_dim where account_name < "h"  ;


update cc_account
set description = "test 4"
where name < "r"

update account_dim
set account_description = "old description "
where account_name < "r"


select count(*) from cc_account where description = "test" 
select count(*) from account_dim where account_Description = "test" 


select * from change_log_ref

-- check
select clf.Data_Change_Message, clf.Effective_Datetime, ad.* 
from change_log_ref  clf
inner join account_dim ad
on clf.record_id = ad.account_id
and ad.effective_datetime = clf.Effective_Datetime
order by ad.account_id, active_flag desc



select * from change_log where Account_Description_Changed = 1 ; 

select account_description , description, name
from account_dim ad
inner join cc_account cc
on ad.account_id = cc.id
where account_description <> description


update cc_account set description = "test"

update account_dim set account_description = "not test"




select * from Existing_Dim_IDs ; 



select * from change_log_ref


Select * from cc_account where description = "test"

2012-06-13 06:59:08


Select * from account_dim where account_id = 18380597

2012-06-13 06:59:08

select 1076 - 747
