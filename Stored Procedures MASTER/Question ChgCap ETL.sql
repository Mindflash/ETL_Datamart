
use pacifica ; 

-- Change Capture E/L    For Question

/*
Design objective is to extract data since the last control date with the lowest performance impact on the product table possible.

*/








/************ Stored Proc for ETL  **********************/

DROP PROCEDURE IF EXISTS Question_ChgCap_ETL;

DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Question_ChgCap_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
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
	SET previous_extract_datetime =  (select last_extract_datetime from Control_Change_Capture where table_name = "Question")  ; 
	SET sp_result = 0;

-- MAIN PROCESS

	-- Truncate CC_Question
	Truncate Table CC_Question; 

	-- Insert to Change Capture (CC) Staging Table
		INSERT INTO CC_Question
		SELECT   
			new_extract_datetime as ETL_Run_Datetime,		-- record the datetime when this insert occurred.
			previous_extract_datetime,    	-- record the prior extract datetime and the new extract datetime.
			ID,
			AccountID,
			Created,
			Modified,
			Deleted,
			QuizID,
			Type,
			QuestionInfoID,
			Title,
			PointValue,
			PartialCorrect,
			AttemptsAllowed,
			DisplayIndex,
			IsValid,
			Product,
			Feedback

		FROM Question  
        WHERE Modified > Previous_Extract_Datetime   ;


	-- If Insert succeeded, then update the control table for this Question.
		Update Control_Change_Capture 
		Set Last_Extract_Datetime = New_Extract_Datetime
		where table_name = "Question" ; 


COMMIT;

END$$
DELIMITER ;



-- End of script