use beaches ; 

select count(*) as userrecord from userrecord ; 


select count(*) as series from course where type = 106000400 ;    --  series 


select count(*) as courses from course where type != 106000400 ;    -- courses


select count(*) as framestate from framestate ; 


select count(*) as frame from frame ;


select count(*) as seriesstates from coursestate cs inner join course c on cs.courseid = c.id  where c.type = 106000400 ;


select count(*) as coursestates from coursestate cs inner join course c on cs.courseid = c.id  where c.type != 106000400 ;


select count(*) as questionanswer  from questionanswer ; 


select count(*) from traineecertificate ; 


select count(*) as question from question ; 


select count(*) as quiz from quiz ;
 