use pacifica; 

Drop Table if exists  Change_Log; 

Create Table  Change_Log (
ID int not null auto_increment,
Table_Name varchar(25), 
Record_ID bigint,
Source_Record_ID bigint,
Effective_Datetime datetime,
Last_Modified_Datetime datetime,
Data_Change_Message varchar(255) ,
Primary Key (ID) ) ;



/*
Goal:  To capture a descriptive message of data changes that occur from one state of a record to another.  

It is not to capture the beginning state without a change.


*/


select * from  change_LOg; 





