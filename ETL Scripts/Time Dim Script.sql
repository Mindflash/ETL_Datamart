use pacifica ; 


-- credit to Akom's Tech Ruminations
-- http://tech.akom.net/archives/36-Creating-A-Basic-Date-Dimension-Table-in-MySQL.html

-- sl: modified to change the grain to minute from second.  Takes ~ 3 hours to load 12 years.

DROP TABLE IF EXISTS Time_Dim;
CREATE TABLE IF NOT EXISTS Time_Dim  (
    Time_ID INT NOT NULL auto_increment,
    Time time,
    Hour int,
    Minute int,
   --  second int,
    AM_PM varchar(2),
    Time_Of_Day varchar(12),
    PRIMARY KEY(time_id)
) ENGINE=InnoDB AUTO_INCREMENT=1000;


delimiter //

DROP PROCEDURE IF EXISTS timedimbuild;
CREATE PROCEDURE timedimbuild ()
BEGIN
    DECLARE v_full_date DATETIME;

    DELETE FROM time_dim;

    SET v_full_date = '2009-01-01 00:00:00';
    WHILE v_full_date <= '2009-01-01 23:59:00' DO

        INSERT INTO Time_Dim (
            time ,
            hour ,
            minute ,
       --     second ,
            am_pm
        ) VALUES (
            TIME(v_full_date),
            HOUR(v_full_date),
            MINUTE(v_full_date),
       --    SECOND(v_full_date),
            DATE_FORMAT(v_full_date,'%p')
        );

        SET v_full_date = DATE_ADD(v_full_date, INTERVAL 1 MINUTE);
    END WHILE;
END;

//
delimiter ;

-- call the stored procedure
call timedimbuild();




select * from time_dim;

SELECT * FROM DATE_DIM; 


-- insert null default value
Delete from Time_Dim where time_id = -1; 
Insert into Time_Dim (time_id, time, Time_Of_Day)
Select -1, cast(-1 as time) , 'TIME FIELD EMPTY';



-- Set Time_Of_Day values
Update Time_Dim
Set Time_Of_Day = "EarlyMorning"
Where hour in (0,1,2,3,4,5) ; 

Update Time_Dim 
Set Time_Of_Day = "Morning"
Where hour in (6,7,8,9,10,11) ; 

Update Time_Dim 
Set Time_Of_Day = "Afternoon"
Where hour in (12,13,14,15,16,17) ;  

Update Time_Dim 
Set Time_Of_Day = "Evening"
Where hour in (18,19,20,21,22,23) ;  

	-- Update Time_Dim set Time_Of_Day = Null ; 
	-- SELECT * FROM TIME_Dim where time_of_day is null ;


-- one day takes 2.56 seconds
-- select 2.56 * 356 / 60  --  15 minutes for a year.  10 years... 2.5 hours




-- Let's see what time functions are available in the reporting tool before solving for this.
select * from time_dim

select date_format( now(), '%H:%i')

Select t.time_of_day, count(*)
from account_dim a
left outer join time_dim t
on date_format(a.first_added_datetime, '%H:%i') = date_format(t.time , '%H:%i')
group by t.time_of_day
; 








-- Take a look  SELECT * FROM TIME_Dim ;



-- End of script
