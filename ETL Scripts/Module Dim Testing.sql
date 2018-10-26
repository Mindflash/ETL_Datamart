use pacifica ; 




-- TEST 1 - COMPLETE LOAD

/*** perform a full load of the Module table into Module_Dim ***/

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "CModule"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime, status)
select "CModule", cast("1900-01-21 00:00:00" as datetime), 0   ;


CALL CModule_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
 
 select count(*) from cc_CModule; 


-- Remove all
Truncate Table Module_Dim ; 

-- call the stored procedure
CALL Module_StarSchema_ETL (@STATE, @MESSAGE);  SELECT @STATE;


-- Check Change Log
select data_change_message, table_name, effective_datetime, max(id), count(*) from Change_Log  group by data_change_message, table_name, effective_datetime



-- Confirm Count
select count(*) from Module_Dim; 




-- TEST 2, CHANGE SOME RECORDS IN THE CONTROL CAPTURE TABLE TO SEE IF THEY CREATE NEW RECORDS IN THE DIMENSION

-- Use the existing cc_Module data above, but change some of the values.
update cc_cModule
set name = "changed to new Module name"
where name < "r" ;

 


-- call the stored procedure
CALL Module_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;


-- Check Change Log
select data_change_message, table_name, effective_datetime, max(id), count(*) from Change_Log  group by data_change_message, table_name, effective_datetime ;



-- View Change Report on Module_Dim data.    THIS QUERY PRODUCES DUPLICATES IN TESTING BECAUSE THE EFFECTIVE-DATETIME DOES NOT CHANGE, WHILE IN PRACTICE IT WOULD.
select c.data_change_message, a.*
from module_dim a
left outer join change_log c
on c.record_id = a.id
and c.effective_datetime = a.etl_run_datetime
order by a.module_id, a.active_flag desc ;





