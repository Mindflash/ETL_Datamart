
use pacifica ; 

-- Change Capture E/L    For Quiz_Fact

/*
Design objective is to extract data since the last control date with the lowest performance impact on the product table possible.

The goal for FrameState ETL is to get all data needed from fact tables that is sourced from Framstate.  Inlcudes  Quiz and Answers.

*/




-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "FrameState"; 

insert into Control_Change_Capture
(Table_name, last_extract_datetime )
select "FrameState", cast("1900-01-21 00:00:00" as datetime)   ;

select * from control_change_capture







/********* source data analysis ***********/

select * from framestate limit 100 ;

select * from frame  limit 100 ;



-- Framestate status
select distinct m.name 
from framestate fs
inner join mtype m
on fs.status = m.id 
limit 100 ;

	/* 
	None
	Started
	Completed
	*/


-- Frame Types
select distinct  m.name , f.type
from frame f
inner join mtype m
on f.type = m.id
 limit 100 ;

	/*
File	108000100
FirstFrame	108000200
Question	108000300
QuizFirstFrame	108000400
QuizLastFrame	108000500
LastFrame	108000600
SurveyIntroFrame	108000700
SurveyQuestionFrame	108000800
SurveyLastFrame	108000900
SCOQuizFrame	108001000
SCOFrame	108001100
	*/

108000800
108001000, 108000500, 108000400

108000300, 108000500          --     Question, QuizLastFrame



select * from cc_framestate where frametype = 108001000

select m.name, q.* from question q inner join mtype m on m.id = q.type  ; 

select m.name, f.* from cc_framestate f inner join mtype m on m.id = f.frametype ;

select * from framestate fs inner join frame f on fs.frameid = f.id limit 100 ; 


/***********    DDL    ***********/


Drop Table If Exists cc_FrameState ; 

CREATE TABLE cc_FrameState (
  ETL_Run_Datetime datetime,
  Last_Extract_Datetime datetime, 

  `FrameStateID` bigint(20) NOT NULL,
  `FrameID` bigint(20) NOT NULL,
  `AccountID` bigint(20) NOT NULL,
  `QuizID` bigint(20) NOT NULL,    		-- From Frame Table
  `QuestionID` bigint(20) NOT NULL,   	-- From Frame Table
    
  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL,
  `Deleted` datetime DEFAULT NULL,
  
  `UserID` bigint(20) NOT NULL,     
  `CModuleID` bigint(20) NOT NULL,    
  `CourseID` bigint(20) NOT NULL,

  `FrameType`  int(11) NOT NULL,   		-- From Frame Table
  `QuestionType`   varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,   		-- From Frame Table
  
  -- The following are all from the Framestate table
  `Status` int(11) NOT NULL,  
  `QuestionPoints` int(11) NOT NULL DEFAULT '0',
 --  `QuestionAnswer` longblob,
  `QuestionIsCorrect` tinyint(1) NOT NULL DEFAULT '0',
  `QuestionAttempts` int(11) NOT NULL DEFAULT '0',
  `QuestionListRandomized` varchar(10000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `Score` int(11) DEFAULT NULL,
  `GradeStatus` int(11) NOT NULL DEFAULT '0',
  `Product` int(11) NOT NULL DEFAULT '140000100',
  `Duration` int(11) DEFAULT '0',
  `ProgressSeconds` int(11) DEFAULT NULL,
  `EngagementScore` int(11) DEFAULT NULL,
  `ScoProperties` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
   AnswerIndexSelected int(11) DEFAULT NULL,
   EssayAnswerLength  int(11) DEFAULT NULL
   
) ENGINE=InnoDB DEFAULT CHARSET=utf8;






/************ Stored Proc for ETL  **********************/

DROP PROCEDURE IF EXISTS Framestate_ChgCap_ETL;
DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Framestate_ChgCap_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN


-- DECLARATIONS
	DECLARE new_extract_datetime datetime; 
	DECLARE previous_extract_datetime datetime;
		
	-- SQLEXCEPTION DECLARATION
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN	
			-- ERROR
            SET SP_Result = 1 ;
            SET SP_MESSAGE = 'An error has occurred, operation rollbacked and the stored procedure was terminated';
			ROLLBACK;
		END;
		

    
-- START TRANSACTION    
	START TRANSACTION;


-- SET VARIABLES
	SET new_extract_datetime = now(); 
	SET previous_extract_datetime =  (select last_extract_datetime from Control_Change_Capture where table_name = "FrameState")  ; 
	SET sp_result = 0;


-- CREATE REF DATA
	Drop Table if exists mtype_2;
	Create Temporary Table mtype_2
	(  ID int(11) NOT NULL,
	  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL 
	) ;
	Insert into mtype_2 Select ID, name from mtype_ref;


-- MAIN PROCESS

	-- Truncate CC_Quiz_Fact
		Truncate Table CC_Framestate; 

	-- Insert to Change Capture (CC) Staging Table


		-- Could use multiple passes for different casees, such as Quiz, T/F question, Mulitple Choice Questions, etc.  
			 INSERT INTO CC_Framestate
             (
				ETL_Run_Datetime,
				Last_Extract_Datetime,
				FrameStateID,
				FrameID,
				AccountID,
				QuizID,
				QuestionID,
				Created,
				Modified,
				Deleted,
				UserID,
				CModuleID,
				CourseID,
				FrameType,     
                QuestionType,
				Status,
				QuestionPoints,
				QuestionIsCorrect,
				QuestionAttempts,
				QuestionListRandomized,
				Score,
				GradeStatus,
				Product,
				Duration,
				ProgressSeconds,
				EngagementScore,
				ScoProperties,
                AnswerIndexSelected,
                EssayAnswerLength
				)
                
			 Select Distinct

-- 				new_extract_datetime, 
-- 				previous_extract_datetime,
		
				fs.ID,
				fs.FrameID,
				fs.AccountID,
				f.QuizID,
				f.QuestionID,
				fs.Created,
				fs.Modified,
				fs.Deleted,
				fs.UserID,
				fs.CModuleID,
				fs.CourseID,
				f.Type,
				m.name,
				fs.Status,
				fs.QuestionPoints,
				fs.QuestionIsCorrect,
				fs.QuestionAttempts,
				fs.QuestionListRandomized,
				fs.Score,
				fs.GradeStatus,
				fs.Product,
				fs.Duration,
				fs.ProgressSeconds,
				fs.EngagementScore,
				fs.ScoProperties,
				
				Case When substring(fs.scoproperties, instr(fs.scoproperties,"AnswerIndex") + 14 ,  1 ) = "," and m.name = "MultipleChoice"  Then cast(substring(fs.scoproperties, instr(fs.scoproperties,"AnswerIndex") + 13 ,  1 )  as UNSIGNED)
					 When substring(fs.scoproperties, instr(fs.scoproperties,"AnswerIndex") + 13 ,  1 ) = "-" and m.name = "MultipleChoice"  Then cast(substring(fs.scoproperties, instr(fs.scoproperties,"AnswerIndex") + 13 ,  2 )   as UNSIGNED)
				End as AnswerIndexSelected ,
				
				ifnull(length(qa.essaytext),0)
            
			 from Framestate fs
			 
			 inner join frame f
			 on fs.frameid = f.id
             
             
             left outer join question q            -- if it is a question record then get the question type and look it up on mtype
             on f.questionid = q.id
             
             left outer join mtype m
             on m.id = q.type
             
             
             left outer join questionanswer qa		-- if it is a question essay record then get the essay answer to calc length
             on qa.framestateid = fs.id
                         
             where fs.modified > previous_extract_datetime 
             
             and f.type in ( 108000300, 108000500 )		-- 108000500    Just get Frame Types that identify the last frame Quiz activity. And Quesiton.
 
            ;
        
       

	-- If Insert succeeded, then update the control table for this Quiz_Fact.
		Update Control_Change_Capture 
		Set Last_Extract_Datetime = New_Extract_Datetime
		where table_name = "Framestate" ; 


COMMIT;

END$$
DELIMITER ;




/************ END Stored Proc for ETL  **********************/


 
 
 select * from answer_dim where question_type = "Mchoice" ; 
 
 
 
 
 
 
 
 
 
 

select questionid, f.type, status, scoproperties, 

Case When  substring(substring(fs.scoproperties, 100,50 ) ,  instr(substring(fs.scoproperties, 100,50 ),"Answer") + 8 , 4)  = "true" then "True"
	 When  substring(substring(fs.scoproperties, 100,50 ) ,  instr(substring(fs.scoproperties, 100,50 ),"Answer") + 8 , 4)  = "fals" then "False"
End
 

 from Framestate fs
 
 inner join frame f
 on fs.frameid = f.id
 
 inner join question q
 on f.questionid = q.id
 
 inner join mtype m
 on m.id = q.type
     
 where m.name = "TrueFalse" 
     
 and scoproperties like "%Answer%"
     
order by f.questionid
  
         
limit 10000 ;       
             
             
             

-- Set Control Datetime for testing
delete from control_change_Capture  where table_name = "Framestate"; 
insert into Control_Change_Capture
(Table_name, last_extract_datetime )
select "Framestate", cast("1900-08-29 23:05:15" as datetime)   ;


truncate table cc_framestate; 

-- call the stored procedure
CALL Framestate_ChgCap_ETL (@STATE, @MESSAGE);    sELECT @STATE;
  


-- Look at cc_Quiz_Fact table

Select * from cc_framestate ; 
    
    
    
    Select * from  framestate order by frameid; 
    
    select fs.scoproperties, 
      substring(fs.scoproperties, instr(fs.scoproperties,"AnswerIndex") + 13 ,  2 )  
     from framestate fs
     
     order by questionid
     
     
    
-- Look at Control_Change_Capture table     
select * from Control_Change_Capture ; 



-- ReSet Control Datetime for testing
delete from control_change_Capture  where table_name = "Quiz_Fact"; 

insert into Control_Change_Capture
(Table_name, last_extract_datetime, status)
select "Quiz_Fact", cast("2018-08-29 23:05:15" as datetime), 0   ;

select * from Control_Change_Capture ;

truncate table cc_Quiz_Fact ; 

select count(*) from Quiz_Fact;

select count(*) from cc_Quiz_Fact;


/*  NOTES

Consider performing a de-dup on the cc table to address risk.  But do not do it in this proc that is querying the production TMS db.  


*/



-- End of script