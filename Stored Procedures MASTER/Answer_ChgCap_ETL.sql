
use pacifica ; 

-- Change Capture E/L    For Answer

/*



*/



/************ Stored Proc for ETL  **********************/

DROP PROCEDURE IF EXISTS Answer_ChgCap_ETL;
DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Answer_ChgCap_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
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
	SET previous_extract_datetime =  (select last_extract_datetime from Control_Change_Capture where table_name = "Answer")  ; 
	SET sp_result = 0;


-- MAIN PROCESS

	-- Truncate CC_Answer
		Truncate Table CC_Answer; 

	-- Insert to Change Capture (CC) Staging Table


		-- The T/F Pass
			 INSERT INTO CC_Answer
             
			 Select Distinct
             new_extract_datetime,
     	     previous_extract_datetime,
             
			 q.AccountID as Account_ID,  
			 q.quizid as Quiz_ID,
             q.ID as Question_ID,  
			 q.displayindex as Index_ID,
             cast(concat(cast(q.ID as char) ,cast((q.displayindex * 10 )as char)) as UNSIGNED)   as Answer_ID,
             q.created as First_Added_Datetime,  
             q.modified as Last_Modified_Datetime, 
             q.deleted as Deleted_Datetime,
             'TF' as Question_Type, 
			 1 as Correct_Answer_Flag,
             
             Case 
				  -- T/F
				  When t.answer = 1  Then  'True' 
				  When t.answer = 0  Then 'False'  
			 End as Answer_Value,
             
             "N/A" as CombinationAnswer,
             
             "N/A" as SequenceAnswer

			 from question q							-- get question type
			 
			 inner join questiontruefalse t		-- get correct answer
			 on q.questioninfoid = t.id
             
			 where q.type = 118000800  
             
             and q.modified > previous_extract_datetime  ;
 
           
   
		-- The Multiple Correct Pass
			 INSERT INTO CC_Answer
             
             Select Distinct
			 new_extract_datetime,
     	     previous_extract_datetime,
			 q.AccountID as Account_ID,  
			 q.quizid as Quiz_ID,
             q.ID as Question_ID,  
			 r.displayindex as Index_ID,
             cast(concat(cast(q.ID as char) ,cast((r.displayindex * 10 )as char)) as UNSIGNED)   as Answer_ID,
             q.created as First_Added_Datetime,  
             q.modified as Last_Modified_Datetime, 
             q.deleted as Deleted_Datetime,
             'MCorrect' as Question_Type, 
			 r.IsAnswer as Correct_Answer_Flag,
			 r.description as Answer_Value,
             "N/A" as CombinationAnswer,
             
             "N/A" as SequenceAnswer
             
			 from question q							-- get question type
		     inner join questionmultiplecorrectentry r    -- get correct answer
		     on q.questioninfoid = r.questionid
			 where q.type = 118000700 
             and q.modified > previous_extract_datetime  ;
             
             
             
             
             
             
		-- Multiple Choice Pass
        
   			 INSERT INTO CC_Answer

             Select Distinct
	  		 new_extract_datetime,
       	     previous_extract_datetime,
			 q.AccountID as Account_ID,  
			 q.quizid as Quiz_ID,
             q.ID as Question_ID,  
			 r.displayindex as Index_ID,
             cast(concat(cast(q.ID as char) ,cast((r.displayindex * 10 )as char)) as UNSIGNED)   as Answer_ID,
             q.created as First_Added_Datetime,  
             q.modified as Last_Modified_Datetime, 
             q.deleted as Deleted_Datetime,
             'MChoice' as Question_Type, 
             r.isanswer as Correct_Answer_Flag,    -- Only flagged if there is only one correct answer
             r.description  as  Answer_Value,
               
             Case When c.answerisall = 1 and c.answerisnone = 0 Then "All Of The Above"
				  When c.answerisall = 0 and c.answerisnone = 1 Then "None Of The Above"
                  When c.answerisall = 1 and c.answerisnone = 1 Then "All Of The Above & None Of The Above"
				  Else "N/A"
			 End  as  CombinationAnswer,
             
             "N/A" as SequenceAnswer
             
			 from question q							-- get question type
		     inner join questionmultiplechoiceentry r    -- get correct answer
		     on q.questioninfoid = r.questionid			-- get combination answer notes
             
             inner join questionmultiplechoice c
             on q.questioninfoid = c.id
             
			 where q.type = 118000100 
             
             and q.modified > previous_extract_datetime  ;
             
             
             
             
		-- Answer Sequence Pass
			INSERT INTO CC_Answer
                 
			Select Distinct  
            
             new_extract_datetime,
       	     previous_extract_datetime,
			 q.AccountID as Account_ID,  
			 q.quizid as Quiz_ID,
             q.ID as Question_ID,  
			 qse.displayindex as Index_ID,
             cast(concat(cast(q.ID as char) ,cast((qse.displayindex * 10 )as char)) as UNSIGNED)   as Answer_ID,
             q.created as First_Added_Datetime,  
             q.modified as Last_Modified_Datetime, 
             q.deleted as Deleted_Datetime,
             'ASequence' as Question_Type, 
             0 as Correct_Answer_Flag,    -- Only flagged if there is only one correct answer
			 qse.description  as  Answer_Value,
               
             "N/A" as CombinationAnswer,
             
             qse.displayindex as SequenceAnswer
            
            
			FROM question q
			JOIN questionsequenceentry qse ON qse.questionid=q.questioninfoid
			WHERE q.type = 118000200  ;
  
             


		-- ImageCaption, ImageParts, Essay Questions.   Just capturing descriptive 

			/*
			These three types we are not capturing the correct answers for, since that would require displaying the image and sequence displayed somehow.   Not in initial scope.alter
            So we are just capturing the basic reference information, such as teh question_type.
            
				Essay			118000900
				ImageCaption	118000300
				ImageParts		118000400
			*/
            
            
			INSERT INTO CC_Answer
                 
			Select     distinct  
            
             new_extract_datetime,
      	     previous_extract_datetime,
			 q.AccountID as Account_ID,  
			 q.quizid as Quiz_ID,
             q.ID as Question_ID,  
			 0 as Index_ID,
             q.ID   as Answer_ID,
             q.created as First_Added_Datetime,  
             q.modified as Last_Modified_Datetime, 
             q.deleted as Deleted_Datetime,
             
             Case When q.type = 118000900  Then  'Essay' 
				  When q.type = 118000300  Then 'ImageCaption'
                  When q.type = 118000400  Then 'ImageParts'
             End as Question_Type, 

             0 as Correct_Answer_Flag,    
             
			 "N/A"  as  Answer_Value,
               
             "N/A" as CombinationAnswer,
             
             0 as SequenceAnswer
                 
			FROM question q

			WHERE q.type in (118000900,118000300, 118000400)    ;


	-- If Insert succeeded, then update the control table for this Answer.
		Update Control_Change_Capture 
		Set Last_Extract_Datetime = New_Extract_Datetime
		where table_name = "Answer" ; 


COMMIT;

END$$
DELIMITER ;






-- End of script