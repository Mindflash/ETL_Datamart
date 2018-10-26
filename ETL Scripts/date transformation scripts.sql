use pacifica;


-- Goal is to store the Date_ID value from the date_dim.

-- And handle NULL date values consistently.   Use -1 for all NULL values.



select * from date_dim where date_id = -1

select * from question_dim; 

select * from course_dim

select * from date_dim 

select * from course_dim ; 

select cast(now() as date) 

select * from account_dim ; 


Select dd.date_id, dd.date,  cast(cd.content_Modified_Datetime as date), cd.content_Modified_Datetime
From date_dim dd
inner join course_dim cd
on cast(cd.content_Modified_Datetime as date) = dd.date



Update course_dim
set content_Modified_Datetime = -1
Where content_Modified_Datetime is null; 






