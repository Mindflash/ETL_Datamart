use pacifica ; 




-- TEST 1 - COMPLETE LOAD

/*** perform a full load of the Course table into Course_Dim ***/

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "Course"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime, status)
select "Course", cast("1900-01-21 00:00:00" as datetime), 0   ;


CALL Course_ChgCap_ETL (@STATE, @MESSAGE);    SELECT @STATE;
 
 select count(*) from cc_course; 

-- Remove all
Truncate Table Course_Dim ; 
 
-- call the stored procedure
CALL Course_StarSchema_ETL (@STATE, @MESSAGE);  SELECT @STATE;


-- Check Change Log
select data_change_message, table_name, effective_datetime, max(id), count(*) from Change_Log  group by data_change_message, table_name, effective_datetime




-- Confirm Count
select count(*) from Course_Dim; 




-- TEST 2, CHANGE SOME RECORDS IN THE CONTROL CAPTURE TABLE TO SEE IF THEY CREATE NEW RECORDS IN THE DIMENSION

-- Use the existing cc_Course data above, but change some of the values.
update cc_Course
set name = "changed to new course name"
where name < "r" ;




-- call the stored procedure
CALL Course_StarSchema_ETL (@STATE, @MESSAGE);    SELECT @STATE;


-- Check Change Log
select data_change_message, table_name, effective_datetime, max(id), count(*) from Change_Log  group by data_change_message, table_name, effective_datetime ;




-- View Change Report 
select c.data_change_message, a.*
from course_dim a
left outer join change_log c
on c.record_id = a.id
and c.effective_datetime = a.etl_run_datetime
order by a.course_id, a.active_flag desc ;









-- Confirm Count
select count(*) from Course_Dim; 






select * from Course_dim; 

select * from Course_dim where active_flag = 1; 

select * from cc_Course ; 

select * from Existing_Dim_IDs ; 

truncate table Course_dim ;


select * from Course_dim where Course_name < "h"  -- 747
delete from Course_dim where Course_name < "h"  ;


update cc_Course
set description = "test 4"
where name < "r"

update Course_dim
set Course_description = "old description "
where Course_name < "r"


select count(*) from cc_Course where description = "test" 
select count(*) from Course_dim where Course_Description = "test" 


select * from change_log_ref

-- check
select clf.Data_Change_Message, clf.Effective_Datetime, ad.* 
from change_log_ref  clf
inner join Course_dim ad
on clf.record_id = ad.Course_id
and ad.effective_datetime = clf.Effective_Datetime
order by ad.Course_id, active_flag desc



select * from change_log where Course_Description_Changed = 1 ; 

select Course_description , description, name
from Course_dim ad
inner join cc_Course cc
on ad.Course_id = cc.id
where Course_description <> description


update cc_Course set description = "test"

update Course_dim set Course_description = "not test"




select * from Existing_Dim_IDs ; 



select * from change_log_ref


Select * from cc_Course where description = "test"

2012-06-13 06:59:08


Select * from Course_dim where Course_id = 18380597

2012-06-13 06:59:08

select 1076 - 747
