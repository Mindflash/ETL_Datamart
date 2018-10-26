use pacifica ; 

drop table if exists Control_Change_Capture ; 

create table Control_Change_Capture (ID INT NOT NULL auto_increment, Table_Name varchar(25), Last_Extract_Datetime  datetime, PRIMARY KEY(ID)
) ENGINE=InnoDB AUTO_INCREMENT=1;



-- test
insert into Control_Change_Capture
(Table_name, last_extract_datetime, status)
select "Account", now()   ;


select * from Control_Change_Capture