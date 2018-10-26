use pacifica ; 

/* Adapted from Tom Cunningham's 'Data Warehousing with MySql' (www.meansandends.com/mysql-data-warehouse) */

###### small-numbers table
DROP TABLE IF EXISTS numbers_small;
CREATE TABLE numbers_small (number INT);
INSERT INTO numbers_small VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

###### main numbers table
DROP TABLE IF EXISTS numbers;
CREATE TABLE numbers (number BIGINT);
INSERT INTO numbers
SELECT thousands.number * 1000 + hundreds.number * 100 + tens.number * 10 + ones.number
  FROM numbers_small thousands, numbers_small hundreds, numbers_small tens, numbers_small ones
LIMIT 1000000;

###### date table
DROP TABLE IF EXISTS dates;
CREATE TABLE dates (
  date_id          BIGINT PRIMARY KEY, 
  date             DATE NOT NULL,
--  timestamp        BIGINT NOT NULL, 
  weekend          CHAR(10) NOT NULL DEFAULT "Weekday",
  day_of_week      CHAR(10) NOT NULL,
  month            CHAR(10) NOT NULL,
  month_day        INT NOT NULL, 
  year             INT NOT NULL,
  week_starting_monday CHAR(2) NOT NULL,
  UNIQUE KEY `date` (`date`),
  KEY `year_week` (`year`,`week_starting_monday`)
);



###### populate it with days
INSERT INTO dates (date_id, date)
SELECT number, DATE_ADD( '2000-01-01', INTERVAL number DAY )
  FROM numbers
  WHERE DATE_ADD( '2000-01-01', INTERVAL number DAY ) BETWEEN '2000-01-01' AND '2030-01-01'
  ORDER BY number;

###### fill in other rows
UPDATE dates SET
--  timestamp =   UNIX_TIMESTAMP(date),
  day_of_week = DATE_FORMAT( date, "%W" ),
  weekend =     IF( DATE_FORMAT( date, "%W" ) IN ('Saturday','Sunday'), 'Weekend', 'Weekday'),
  month =       DATE_FORMAT( date, "%M"),
  year =        DATE_FORMAT( date, "%Y" ),
  month_day =   DATE_FORMAT( date, "%d" );

UPDATE dates SET week_starting_monday = DATE_FORMAT(date,'%v');



select * from dates  limit 100 ;



###### Date_Dim table
DROP TABLE IF EXISTS Date_Dim;
CREATE TABLE Date_Dim (
  Date_ID          BIGINT PRIMARY KEY, 
  Date             DATE NOT NULL,
  Date_Label       Char(20) Not Null,
 -- Timestamp        BIGINT NOT NULL, 
  Day_Of_Week      CHAR(10) NOT NULL,
  Month_Day        int Not Null,
  Weekend          CHAR(10) NOT NULL DEFAULT "Weekday",
  Holiday_Flag     Int Default 0,
  Month_Desc       CHAR(10) NOT NULL,
  Month_Number     Int  Not Null,
  Year_Desc        Char(4) Not Null,
  Year_Number      INT NOT NULL,
  Quarter_Desc     Char(2) Not NUll,
  Quarter_Number    Int not null,
  Week_Starting_Monday CHAR(2) NOT NULL,
  UNIQUE KEY `date` (`date`)
);


-- Populate Date_Dim
Insert Into Date_Dim
Select 
Date_ID, 
Date,
DATE_FORMAT(cast(date as char), "%M %d %Y") as Date_Label,
-- TimeStamp,
Day_Of_Week,
month_day as Month_Day,
Weekend, 
0 as Holiday_Flag,
Month as Month_Desc,
DATE_FORMAT(cast(date as char), "%m") as Month_Number,
DATE_FORMAT(cast(date as char), "%y") as Year_Number,
year as Year_Desc,
Case When DATE_FORMAT(cast(date as char), "%m") in (1,2,3) then "Q1"
	 When DATE_FORMAT(cast(date as char), "%m") in (4,5,6) then "Q2"
	 When DATE_FORMAT(cast(date as char), "%m") in (7,8,9) then "Q3"
     Else "Q4"
End as Quarter_Desc,
Case When DATE_FORMAT(cast(date as char), "%m") in (1,2,3) then 1
	 When DATE_FORMAT(cast(date as char), "%m") in (4,5,6) then 2
	 When DATE_FORMAT(cast(date as char), "%m") in (7,8,9) then 3
     Else 4
End as Quarter_Number,
Week_Starting_Monday 
from dates order by date  ; 



-- Set NULL Default Value 
Insert Into Date_Dim  (DATE_ID, DATE, DATE_LABEL)
Select -1, '1900-01-01', 'DATE FIELD EMPTY';


-- Set Holiday Flag.   Only done for fixed dates.
Update Date_Dim
Set Holiday_Flag = 1
Where  (month_number = 1 and month_day = 1 )
or (month_number = 7 and month_day = 4 )
or  ( month_number = 12 and month_day = 25 )   ;

-- 2018, 2019, 2020 ...  Memorial Day, Labor Day, Thanksgiving Day , Columbus Day, Veterans Day    ** Needs Business Rule **

Update Date_Dim
Set Holiday_Flag = 1
Where cast( concat( date_format(date, "%m") , "/", date_format(date,"%d"), "/20", date_format(date,"%y") ) as char)    in 
("01/15/2018",	"01/21/2019",	"01/20/2020",
"02/19/2018",	"02/18/2019",	"02/17/2020",
"05/28/2018",	"05/27/2019",	"05/25/2020",
"07/04/2018",	"07/04/2019",	"07/03/2020",
"09/03/2018",	"09/02/2019",	"09/07/2020",
"10/08/2018",	"10/14/2019",	"10/12/2020",
"11/12/2018",   "11/11/2019",    "11/11/2020",
"11/22/2018",	"11/28/2019",	"11/26/2020",
"12/25/2018",	"12/25/2019",	"12/25/2020") ;



-- Drop working tables 
DROP TABLE IF EXISTS numbers_small;
DROP TABLE IF EXISTS numbers;
DROP TABLE IF EXISTS dates;



-- take a look 
select * from date_dim ; 









