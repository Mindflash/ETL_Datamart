use pacifica ; 




-- TEST 1 - COMPLETE LOAD

/*** perform a full load of the Module_Participation table into Module_Participation_Dim ***/

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "CModuleState"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime )
select "CModuleState", cast("1900-01-21 00:00:00" as datetime)    ;

Select * from Control_Change_Capture ;


select count(*) from cc_CModulestate ;

-- call the stored procedure to extract all since the control date.   Note that this will re-set the control date to a new current value.  

-- call the stored procedure
CALL CModuleState_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;



-- confirm
select count(*) from cc_cModulestate ;

select * from Module_Participation_Fact ;


-- Remove all
Truncate Table Module_Participation_Fact ; 

Truncate Table change_log;


-- call the stored procedure
CALL Module_Participation_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;

   
-- Confirm Count
select count(*) from Module_Participation_Fact ;


-- Check Change Log
select * from Change_Log ; 



-- TEST 2, CHANGE SOME RECORDS IN THE CONTROL CAPTURE TABLE TO SEE IF THEY CREATE NEW RECORDS IN THE DIMENSION
 
-- Use the existing cc_Module_Participation data above, but change some of the values.
update cc_cModulestate
set invitedbyuserId = 0
where   substring(invitedbyuserId,1,2) < 14


select * from cc_cModulestate


select substring(invitedbyuserId,1,2)
from  cc_cModulestate
where  substring(invitedbyuserId,1,2) < 14

select * from  Module_Participation_Fact
select description from cc_Module_Participation where name < "r"
select Module_Participation_description from Module_Participation_dim where Module_Participation_name < "r"




-- call the stored procedure
CALL Module_Participation_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;


-- Confirm Count
select count(*) from Module_Participation_Fact; 





select * from Module_Participation_fact order by cmodulestate_id, active_flag desc ; 

-- Check Change Log
select data_change_message, table_name, effective_datetime, max(id), count(*) from Change_Log  group by data_change_message, table_name, effective_datetime ;

select count(*) from change_Log ;
select * from change_Log
Select distinct source_record_id from change_log




-- View Change Report on Module_Dim data.    THIS QUERY PRODUCES DUPLICATES IN TESTING BECAUSE THE EFFECTIVE-DATETIME DOES NOT CHANGE, WHILE IN PRACTICE IT WOULD.
select distinct c.data_change_message, a.*
from Module_Participation_fact a
left outer join change_log c
on c.record_id = a.mpf_id
and c.effective_datetime = a.etl_run_datetime
order by a.cmodulestate_id, a.active_flag desc ;








select * from Module_Participation_dim; 

select * from Module_Participation_dim where active_flag = 1; 

select * from cc_Module_Participation ; 

select * from Existing_Dim_IDs ; 

truncate table Module_Participation_dim ;


select * from Module_Participation_dim where Module_Participation_name < "h"  -- 747
delete from Module_Participation_dim where Module_Participation_name < "h"  ;


update cc_Module_Participation
set description = "test 4"
where name < "r"

update Module_Participation_dim
set Module_Participation_description = "old description "
where Module_Participation_name < "r"


select count(*) from cc_Module_Participation where description = "test" 
select count(*) from Module_Participation_dim where Module_Participation_Description = "test" 


select * from change_log_ref

-- check
select clf.Data_Change_Message, clf.Effective_Datetime, ad.* 
from change_log_ref  clf
inner join Module_Participation_dim ad
on clf.record_id = ad.Module_Participation_id
and ad.effective_datetime = clf.Effective_Datetime
order by ad.Module_Participation_id, active_flag desc



select * from change_log where Module_Participation_Description_Changed = 1 ; 

select Module_Participation_description , description, name
from Module_Participation_dim ad
inner join cc_Module_Participation cc
on ad.Module_Participation_id = cc.id
where Module_Participation_description <> description


update cc_Module_Participation set description = "test"

update Module_Participation_dim set Module_Participation_description = "not test"




select * from Existing_Dim_IDs ; 



select * from change_log_ref


Select * from cc_Module_Participation where description = "test"

2012-06-13 06:59:08


Select * from Module_Participation_dim where Module_Participation_id = 18380597

2012-06-13 06:59:08

select 1076 - 747
