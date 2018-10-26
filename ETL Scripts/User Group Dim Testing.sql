use pacifica ; 




-- TEST 1 - COMPLETE LOAD

/*** perform a full load of the account table into Account_Dim ***/

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "UserGroup"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime, status)
select "UserGroup", cast("2018-09-01 00:00:00" as datetime), 0   ;

Select * from Control_Change_Capture ;



-- call the stored procedure to extract all since the control date.   Note that this will re-set the control date to a new current value.  

-- call the stored procedure
CALL UserGroup_ChgCap_ETL (@STATE, @MESSAGE);   SELECT @STATE;


select * from cc_userGroup; 



truncate table user_group_dim; 

-- call the stored procedure
CALL UserGroup_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;




select * from user_group_dim;




/************ next test :   change records **************/


select * from cc_usergroup


update cc_usergroup
set name = "Charles The Dude"
Where name < "r" ;


-- call the stored procedure
CALL UserGroup_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;




select * from user_group_dim order by user_group_id, active_flag desc;


-- View Change Report on Module_Dim data.    THIS QUERY PRODUCES DUPLICATES IN TESTING BECAUSE THE EFFECTIVE-DATETIME DOES NOT CHANGE, WHILE IN PRACTICE IT WOULD.
select c.data_change_message, a.*
from user_group_dim a
left outer join change_log c
on c.record_id = a.id
and c.effective_datetime = a.etl_run_datetime
order by a.user_group_id, a.active_flag desc ;








