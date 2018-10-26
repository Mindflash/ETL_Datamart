use pacifica; 




/*


just looking at the tables you could figure it out
all the Quiz*tables

Steve Loyd [2:25 PM]
do you mean all the Question tables ?

Josh Larsen [2:26 PM]
yes, those
question*

Steve Loyd [2:27 PM]
ok...  thanks

Josh Larsen [2:29 PM]
```SELECT * 
FROM question q
JOIN questionsequence qs ON q.questioninfoid=qs.id
JOIN questionsequenceentry qse ON qse.questionid=qs.id
WHERE q.id=6656511
ORDER BY qse.displayindex```

that for example shows you the ordering (correct answer) to the question in the scoproperties from above
so the answer was incorrect because they ordered this answer in [2,0,1], it should be [0,1,2] to be correct
as i said, each “type” of question has its own subtleties, but you can probably see them easily (edited)

Steve Loyd [2:32 PM]
You must be in a different db.  I get no results... but I can change the question id...

Josh Larsen [2:32 PM]
montara

Steve Loyd [2:32 PM]
thanks for the hints... make more sense

Josh Larsen [2:32 PM]
the scheme is all the same in pa too
we made no changes in this area

Steve Loyd [2:33 PM]
cool


*/


-- questions with sequences
select * from questionsequence inner join 
8078295
8078296
8108089
8291800
8572274


select * from questionsequenceentry
order by id, displayindex

select * from questionmultiplecorrect


-- correct answer




select m.name, q.*
FROM question q
inner join mtype m
on q.type = m.id




-- multiple choice example
6823863
6866205
7033742
8166179
8166181


SELECT * 
FROM question q
JOIN questionsequence qs ON q.questioninfoid=qs.id
JOIN questionsequenceentry qse ON qse.questionid=qs.id  
WHERE q.id=8572274    -- question id
ORDER BY qse.displayindex



-- Answers
select scoproperties 
from framestate fs
inner join frame f
on fs.frameid = f.id
where f.questionid = 8166181
 
 
 
 select scoproperties from framestate where scoproperties like '%QuizQuestionAnswer%'
 
 
 
 -- question types
 select distinct m.name, q.type
 from question q
 inner join mtype m
 on m.id = q.type  
 
MultipleChoice	118000100
Sequence		118000200
ImageCaption	118000300
ImageParts		118000400
MultipleCorrect	118000700
TrueFalse		118000800
Essay			118000900




 select count(*)
 from question q
 inner join mtype m
 on m.id = q.type
  where m.name = 'TrueFalse'
 -- 200762
 
  select count(*) 
 from question q
 inner join questiontruefalse t
 on q.questioninfoid = t.id
  -- 200762
  
  -- So we know that these question tables are used to identify the type of question, on questioninfoid join.
  
 
 
 /**********    Main Extract Query for Question_Answer_Fact     **************/
 
 select Distinct
  fs.scoproperties ,
 
 fs.AccountID as Account_ID,  fs.UserID as Trainee_ID, fs.CModuleID as Module_ID,  f. QuizID as Quiz_ID, 
 f.QuestionID as Question_ID, fs.id as Framestate_ID, fs.created as First_Added_Datetime,  fs.modified as Last_Modified_Datetime, 
 fs.modified as Answer_Datetime,
 
 Case When q.type = 118000800 and t.answer = 1  Then  'True' Else 'False'  End as Correct_Answer_Value,      -- T/F Question Type
 
 
 fs.scoproperties    as Trainee_Answer_Value
 
 from framestate fs
 inner join frame f
 on fs.frameid = f.id
 inner join question q				-- get question type
 on f.questionid = q.id
 
 left outer join questiontruefalse t		-- get correct answer
 on q.questioninfoid = t.id
 
 where q.type = 118000800    -- testing
 
 limit 100 ; 
 
 
  
 
/************ Essay  **************/
 
 select * from questionessay  limit 100 ; 
 
 select * from question_dim 
 

 select * 
 from questionessay  qe
 inner join question q
 on qe.id = q.questioninfoid
 
 
 limit 100 ; 
 
 select distinct question_type from question_dim
 
 
 select * from question_dim where question_type = "essay"
 
 
 
/************ Answer Squence  **************/

select * from questionsequence   -- just id's these types of questions.  can use type as well

select * from questionsequenceentry    -- all of the answers and the correct display order  displayindex

select count(*) from question
where type = 118000200
9766
select count(q.id) from question q
inner join questionsequence qs
on  q.questioninfoid=qs.id
9766


SELECT * 
FROM question q
JOIN questionsequence qs ON q.questioninfoid=qs.id
JOIN questionsequenceentry qse ON qse.questionid=qs.id
WHERE q.id= 9455600
ORDER BY qse.displayindex





Sequence		118000200

select *
FROM question q
JOIN questionsequence qs ON q.questioninfoid=qs.id
JOIN questionsequenceentry qse ON qse.questionid=qs.id
WHERE q.type = 118000200
ORDER BY qse.displayindex



select *
FROM question q
JOIN questionsequenceentry qse ON qse.questionid=q.questioninfoid
WHERE q.type = 118000200
ORDER BY q.id, qse.displayindex

-- get the correct sequence order for each question
select distinct q.id as QuestionID, qse.id as QSE_ID, qse.description, q.displayindex, qse.displayindex
FROM question q
JOIN questionsequenceentry qse ON qse.questionid=q.questioninfoid
WHERE q.type = 118000200
ORDER BY q.id, qse.displayindex

 
  
/*********   ImageCaption  *********/
 
 
 select * from questionimagecaption
 
 
  
 select * from questionimagecaptionentry  order by questionid, displayindex
 
 
 
 select * from question q
 inner join questionimagecaptionentry qi
 on q.questioninfoid = qi.questionid
 
 order by q.id, qi.displayindex
 
   
/*********   ImageCaptionParts  *********/
 
 
 select * from questionimageparts
 
   
 select * from questionimagepartsentry  order by questionid, displayindex
 
 select * 
 from framestate fs
 inner join frame f
 on fs.frameid = f.id
 inner join question q
 on f.questionid = q.id
  
 limit 100 ; 
 
  
 select * from question q
 inner join questionimagepartsentry qi
 on q.questioninfoid = qi.questionid
 
 order by q.id, qi.displayindex
 
 
 
 
 
 
/********* T/F  *********/
 
use pacifica; 




/*


just looking at the tables you could figure it out
all the Quiz*tables

Steve Loyd [2:25 PM]
do you mean all the Question tables ?

Josh Larsen [2:26 PM]
yes, those
question*

Steve Loyd [2:27 PM]
ok...  thanks

Josh Larsen [2:29 PM]
```SELECT * 
FROM question q
JOIN questionsequence qs ON q.questioninfoid=qs.id
JOIN questionsequenceentry qse ON qse.questionid=qs.id
WHERE q.id=6656511
ORDER BY qse.displayindex```

that for example shows you the ordering (correct answer) to the question in the scoproperties from above
so the answer was incorrect because they ordered this answer in [2,0,1], it should be [0,1,2] to be correct
as i said, each “type” of question has its own subtleties, but you can probably see them easily (edited)

Steve Loyd [2:32 PM]
You must be in a different db.  I get no results... but I can change the question id...

Josh Larsen [2:32 PM]
montara

Steve Loyd [2:32 PM]
thanks for the hints... make more sense

Josh Larsen [2:32 PM]
the scheme is all the same in pa too
we made no changes in this area

Steve Loyd [2:33 PM]
cool


*/


-- questions with sequences
select * from questionsequence inner join 
8078295
8078296
8108089
8291800
8572274


select * from questionsequenceentry
order by id, displayindex

select * from questionmultiplecorrect


-- correct answer




select m.name, q.*
FROM question q
inner join mtype m
on q.type = m.id




-- multiple choice example
6823863
6866205
7033742
8166179
8166181


SELECT * 
FROM question q
JOIN questionsequence qs ON q.questioninfoid=qs.id
JOIN questionsequenceentry qse ON qse.questionid=qs.id  
WHERE q.id=8572274    -- question id
ORDER BY qse.displayindex



-- Answers
select scoproperties 
from framestate fs
inner join frame f
on fs.frameid = f.id
where f.type = 
 
 
 
 select scoproperties from framestate where scoproperties like '%QuizQuestionAnswer%'
 
 
 
 -- question types
 select distinct m.name, q.type
 from question q
 inner join mtype m
 on m.id = q.type  
 
MultipleChoice	118000100
Sequence		118000200
ImageCaption	118000300
ImageParts		118000400
MultipleCorrect	118000700
TrueFalse		118000800
Essay			118000900



 select count(*)
 from question q
 inner join mtype m
 on m.id = q.type
  where m.name = 'TrueFalse'
 -- 200762
 
  select count(*) 
 from question q
 inner join questiontruefalse t
 on q.questioninfoid = t.id
  -- 200762
  
  -- So we know that these question tables are used to identify the type of question, on questioninfoid join.
  
 
 
 
 
 
 /**********    Main Extract Query for Question_Answer_Fact     **************/
 
 -- Run the extract in multiple passes, rather than in one big pass with lots of joins.
 
 
 -- The T/F Pass
 select Distinct
 fs.AccountID as Account_ID,  fs.UserID as Trainee_ID, fs.CModuleID as Module_ID,  f. QuizID as Quiz_ID, 
 f.QuestionID as Question_ID, fs.id as Framestate_ID, fs.created as First_Added_Datetime,  fs.modified as Last_Modified_Datetime, 
 fs.modified as Answer_Datetime,
 
 Case 
	  -- T/F
	  When t.answer = 1  Then  'True' 
	  When t.answer = 0  Then 'False'  
    
End as Correct_Answer_Value,      
 
 fs.scoproperties    ,

 Case
	  -- T/F
	  When substring(scoproperties, 117,  5 )  = "true,"  Then 'True'  
	  When substring(scoproperties, 117,  5 )  = "false"  Then 'False'  
 
End as Trainee_Answer_Value    -- T/F Answer
  
 from framestate fs
 inner join frame f
 on fs.frameid = f.id
 inner join question q				-- get question type
 on f.questionid = q.id
 
 left outer join questiontruefalse t		-- get correct answer
 on q.questioninfoid = t.id
 
 where q.type = 118000800   -- testing
 
 limit 100 ; 
 
 
 
 
 
 
 
 
 
/********* T/F  *********/
 
--  Correct Answer 

  select Distinct
 fs.AccountID as Account_ID,  fs.UserID as Trainee_ID, fs.CModuleID as Module_ID,  f. QuizID as Quiz_ID, 
 f.QuestionID as Question_ID, fs.id as Framestate_ID, fs.created as First_Added_Datetime,  fs.modified as Last_Modified_Datetime, 
 fs.modified as Answer_Datetime,
 
 Case When q.type = 118000800 and t.answer = 1  Then  'True' Else 'False'  End as Correct_Answer_Value,      -- T/F Question Type
 
  fs.scoproperties    ,
 
 Case When  substring(scoproperties, 117,  5 )   = "false" Then 'False' Else 'True'  End as Trainee_Answer_Value    -- T/F Answer

   
 from framestate fs
 inner join frame f
 on fs.frameid = f.id
 inner join question q				-- get question type
 on f.questionid = q.id
 
 left outer join questiontruefalse t		-- get correct answer
 on q.questioninfoid = t.id
 
 where q.type = 118000800    -- testing
 
 limit 100 ; 
 
 
 
 
 
 
/***********  Multiple Correct  ***********/
 
 select * from questionmultiplecorrectentry    -- has records that match to all of the type in questino
 
   select * from questionmultiplecorrect ;
   
   
   
   select count(*) from question q							-- get question type
	 where q.type = 118000700 
   7873
   
   
   
select count(distinct r.questionid) from question q
   			 
 inner join questionmultiplecorrectentry r    -- get correct answer
 on q.questioninfoid = r.questionid
             
             
select count(distinct q.id) from question q
inner join questionmultiplecorrect r    -- get correct answer
on q.questioninfoid = r.id
 
 7873
 
 -- So joining to either table returns all question records of that type.
 
 
             
             
             
             
 -- there may be multiple correct answers
  select * 
  from questionmultiplecorrectentry
  where questionid = 20499196
  
  
  order by questionid, displayindex 
  
  select * from question    where questioninfoid = 19569659 ; 
  

  
  select *
  from question q
  inner join questionmultiplecorrectentry r 
  on q.questioninfoid = r.questionid
 

select * from question_dim
where question_type like "MultipleCorrect"



			 Select Distinct
  --         new_extract_datetime,
  --  	     previous_extract_datetime,
             
			 q.AccountID as Account_ID,  
			 q.quizid as Quiz_ID,
             q.ID as Question_ID,  
			 r.displayindex as Index_ID,
             cast(concat(cast(q.ID as char) ,cast((q.displayindex * 10 )as char)) as UNSIGNED)   as Answer_ID,
             q.created as First_Added_Datetime,  
             q.modified as Last_Modified_Datetime, 
             q.deleted as Deleted_Datetime,

             'MC' as Question_Type, 
			 
			 r.IsAnswer as Correct_Answer_Flag,
             
			 r.description as Answer_Value,
             
			 r.description as  Answer_Description

			 from question q							-- get question type
			 
		     inner join questionmultiplecorrectentry r    -- get correct answer
		     on q.questioninfoid = r.questionid
             
			 where q.type = 118000700 
             
             and q.modified > previous_extract_datetime  ;
 	  
  
  
  
  
  
  
-- The Multiple Choice Pass

select * from cc_answer;

			 select * from questionmultiplechoice


			 select * from questionmultiplechoiceentry

			select includeallabove + includenoneabove + answerisall + answerisnone , a.* from questionmultiplechoice a


			 INSERT INTO CC_Answer
             
             
             Select Distinct
	-- 		 new_extract_datetime,
     -- 	     previous_extract_datetime,
			 q.AccountID as Account_ID,  
			 q.quizid as Quiz_ID,
             q.ID as Question_ID,  
			 r.displayindex as Index_ID,
             cast(concat(cast(q.ID as char) ,cast((r.displayindex * 10 )as char)) as UNSIGNED)   as Answer_ID,
             q.created as First_Added_Datetime,  
             q.modified as Last_Modified_Datetime, 
             q.deleted as Deleted_Datetime,
             'Mc' as Question_Type, 
             
             r.isanswer as Correct_Answer_Flag,    -- Only flagged if there is only one correct answer
             
             r.description  as  Answer_Value,
               
             Case When c.answerisall = 1 and c.answerisnone = 0 Then "All Of The Above"
				  When c.answerisall = 0 and c.answerisnone = 1 Then "None Of The Above"
                  When c.answerisall = 1 and c.answerisnone = 1 Then "All Of The Above & None Of The Above"
				  Else "N/A"
			 End  as  Answer_Combination_Description
             
			 from question q							-- get question type
		     inner join questionmultiplechoiceentry r    -- get correct answer
		     on q.questioninfoid = r.questionid			-- get combination answer notes
             
             inner join questionmultiplechoice c
             on q.questioninfoid = c.id
             
			 where q.type = 118000100 
             
             and q.modified > previous_extract_datetime  ;
  
  
  
  
  
  
  
  -- Trainee_answer_Value
 
 select Distinct
 fs.AccountID as Account_ID,  fs.UserID as Trainee_ID, fs.CModuleID as Module_ID,  f. QuizID as Quiz_ID, 
 f.QuestionID as Question_ID, fs.id as Framestate_ID, fs.created as First_Added_Datetime,  fs.modified as Last_Modified_Datetime, 
 fs.modified as Answer_Datetime,
 
 Case 
       -- Multiple Choice
		when 1= 1 then 'Y'
             
End as Correct_Answer_Value,      
 
 
  fs.scoproperties    ,

 Case
     -- Multiple Choice
	  When q.type = 118000100  and  substring(scoproperties, instr(scoproperties,"AnswerIndex") + 14, 1 )  =  ','    Then substring(scoproperties, instr(scoproperties,"AnswerIndex") + 13, 1 ) 
	  When q.type = 118000100  and  substring(scoproperties, instr(scoproperties,"AnswerIndex") + 13, 2 ) <>  ','    Then substring(scoproperties, instr(scoproperties,"AnswerIndex") + 13, 2 ) 

End as Trainee_Answer_Value    
  
  
 from framestate fs
 inner join frame f
 on fs.frameid = f.id
 inner join question q				-- get question type
 on f.questionid = q.id
 
 left outer join questionmultiplecorrectentry t		-- get correct answer
 on q.questioninfoid = t.id
 
 where q.type = 118000100   -- testing
 
 
 limit 100 ; 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 --  Victor: sample code that parses scoproperties:  https://github.com/Mindflash/mm-report/blob/master/handlers/exportSurveys.js#L164
 
 
 
 use pacifica ; 
 
 select scoproperties from framestate where scoproperties is not null limit 100 ; 

 select distinct substring(scoproperties,1,88)  from framestate order by substring(scoproperties,1,88)  limit 100 ;
 
	 /*
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerEssay, Mindflash
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerImageCaption, Mi
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerImageParts, Mind
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerMultipleChoice, 
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerMultipleCorrect,
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerSequence, Mindfl
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerTrueFalse, Mindf
	 */
     
     
select scoproperties from framestate where scoproperties like '%QuizQuestionAnswer%'

 
 select fs.userid, f.quizid, f.questionid, fs.scoproperties
 from framestate fs
 inner join frame f
 on fs.frameid = f.id
 where scoproperties like '%QuizQuestionAnswer%'
 
 limit 100 ;
 
 
 select 
  -- scoproperties, 
  frameid, courseid, userid, QuestionIsCorrect,

  substring(scoproperties,73,100)  ,
  
  substring(scoproperties,73,  instr(substring(scoproperties,73,35) , ",") - 1 )  as Question_Type ,

  Case When instr(scoproperties,"AnswerIndex") > 0 Then "AnswerIndex"  Else "IndexOrder" End as IndexOrder,

  Case When instr(scoproperties,"AnswerWasNone") > 0 Then "AnswerWasNone" 
       When instr(scoproperties,"Answer") > 0 Then "New Value"
       End as Answer
  
 
 from framestate where scoproperties is not null  

 limit 1000 ; 
 
 
 
 
 create temporary table z_question_type (type varchar(200));
 
 Insert into z_question_type

 select   
  substring(scoproperties,73,  instr(substring(scoproperties,73,35) , ",") - 1 )  as Question_Type 
  from framestate 
 where scoproperties is not null  
 
 
 select * from z_question_type
--  and   substring(scoproperties,55,18)  <> "QuizQuestionAnswer" 
 


 select
 
 scoproperties,
 
 
  substring(substring(scoproperties,73,100),
    instr(substring(scoproperties,73,100) , "TMS.Business" ) + 15 ,
    instr(substring(scoproperties,73,100) , ":" ) -  ( instr(substring(scoproperties,73,100) , "TMS.Business" ) + 16 )
    )  as IndexOrder ,
    
    
  substring(  
	substring(substring(scoproperties,73,100),
    instr(substring(scoproperties,73,100) , "TMS.Business" ) + 15 ,
    100 )  ,
    instr(	substring(substring(scoproperties,73,100),
			instr(substring(scoproperties,73,100) , "TMS.Business" ) + 15 ,
			100 ) ,	":" ) + 4,
		100 )   as nextthing


from framestate fs


limit 100 ; 


select count(*) 
 from framestate where scoproperties is not null  
 
where   substring(scoproperties,55,18)  <> "QuizQuestionAnswer" 


 
 
 
 
 
 
 select * from question
 
 
 select * 
 from questiontruefalse

 
 select count(*) 
 from question q
 inner join questiontruefalse t
 on q.questioninfoid = t.id
  -- 200762
  
  
 inner join mtype m
 on m.id = q.type
 
 where m.name = 'TrueFalse'
 
 
 
 
 select fs.userid, f.quizid, f.questionid, fs.scoproperties
 from framestate fs
 inner join frame f
 on fs.frameid = f.id
 where scoproperties like '%TrueFalse%'
 and questionid = 19925818
 
 
 
 
 
 
 
 
 
 
 
 
 
 --  Victor: sample code that parses scoproperties:  https://github.com/Mindflash/mm-report/blob/master/handlers/exportSurveys.js#L164
 
 
 
 use pacifica ; 
 
 select scoproperties from framestate where scoproperties is not null limit 100 ; 

 select distinct substring(scoproperties,1,88)  from framestate order by substring(scoproperties,1,88)  limit 100 ;
 
	 /*
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerEssay, Mindflash
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerImageCaption, Mi
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerImageParts, Mind
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerMultipleChoice, 
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerMultipleCorrect,
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerSequence, Mindfl
		{"$type":"Mindflash.TMS.Business.Model.SerializedData.QuizQuestionAnswerTrueFalse, Mindf
	 */
     
     
select scoproperties from framestate where scoproperties like '%QuizQuestionAnswer%'

 
 select fs.userid, f.quizid, f.questionid, fs.scoproperties
 from framestate fs
 inner join frame f
 on fs.frameid = f.id
 where scoproperties like '%QuizQuestionAnswer%'
 
 limit 100 ;
 
 
 select 
  -- scoproperties, 
  frameid, courseid, userid, QuestionIsCorrect,

  substring(scoproperties,73,100)  ,
  
  substring(scoproperties,73,  instr(substring(scoproperties,73,35) , ",") - 1 )  as Question_Type ,

  Case When instr(scoproperties,"AnswerIndex") > 0 Then "AnswerIndex"  Else "IndexOrder" End as IndexOrder,

  Case When instr(scoproperties,"AnswerWasNone") > 0 Then "AnswerWasNone" 
       When instr(scoproperties,"Answer") > 0 Then "New Value"
       End as Answer
  
 
 from framestate where scoproperties is not null  

 limit 1000 ; 
 
 
 
 
 create temporary table z_question_type (type varchar(200));
 
 Insert into z_question_type

 select   
  substring(scoproperties,73,  instr(substring(scoproperties,73,35) , ",") - 1 )  as Question_Type 
  from framestate 
 where scoproperties is not null  
 
 
 select * from z_question_type
--  and   substring(scoproperties,55,18)  <> "QuizQuestionAnswer" 
 


 
 
 
  substring(substring(scoproperties,73,100),
    instr(substring(scoproperties,73,100) , "TMS.Business" ) + 15 ,
    instr(substring(scoproperties,73,100) , ":" ) -  ( instr(substring(scoproperties,73,100) , "TMS.Business" ) + 16 )
    )  as IndexOrder ,
    
    
  substring(  
	substring(substring(scoproperties,73,100),
    instr(substring(scoproperties,73,100) , "TMS.Business" ) + 15 ,
    100 )  ,
    instr(	substring(substring(scoproperties,73,100),
			instr(substring(scoproperties,73,100) , "TMS.Business" ) + 15 ,
			100 ) ,	":" ) + 4,
		100 )   as nextthing





select count(*) 
 from framestate where scoproperties is not null  
 
where   substring(scoproperties,55,18)  <> "QuizQuestionAnswer" 


 
 
 