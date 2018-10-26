use pacifica ; 




-- TEST 1 - COMPLETE LOAD

/*** perform a full load of the account table into Account_Dim ***/

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "Product"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime, status)
select "Product", cast("2018-09-01 00:00:00" as datetime), 0   ;

Select * from Control_Change_Capture ;



-- call the stored procedure to extract all since the control date.   Note that this will re-set the control date to a new current value.  

-- call the stored procedure
CALL Product_ChgCap_ETL (@STATE, @MESSAGE); 

SELECT @STATE;
SELECT @MESSAGE ; 

select * from cc_Product; 



truncate table user_dim; 

-- call the stored procedure
CALL Product_StarSchema_ETL (@STATE, @MESSAGE); 

SELECT @STATE;
 

select * from Product_dim;




/************ next test :   change records **************/


select * from cc_Product; 

update cc_Product
set name = "Charles The Dude"
Where name < "r" ;


-- call the stored procedure
CALL User_StarSchema_ETL (@STATE, @MESSAGE); 

SELECT @STATE;

select * from user_dim order by user_id, active_flag desc;



/************ next test :   remove records to simulate new record inserts  **************/

select * from user_dim where first_name like "Train%";

delete from user_dim where first_name like "Train%" ;


-- call the stored procedure
CALL User_StarSchema_ETL (@STATE, @MESSAGE); 

SELECT @STATE;
SELECT @MESSAGE ; 

select * from user_dim order by user_id, active_flag desc;








