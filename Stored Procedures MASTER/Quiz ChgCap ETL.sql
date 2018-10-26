
use pacifica ; 

-- Change Capture E/L    For Quiz





/************ Stored Proc for ETL  **********************/

DROP PROCEDURE IF EXISTS Quiz_ChgCap_ETL;

DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Quiz_ChgCap_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
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
	SET previous_extract_datetime =  (select last_extract_datetime from Control_Change_Capture where table_name = "Quiz")  ; 
	SET sp_result = 0;

-- MAIN PROCESS

	-- Truncate CC_Quiz
	Truncate Table CC_Quiz; 

	-- Insert to Change Capture (CC) Staging Table
		INSERT INTO CC_Quiz
		SELECT   
			new_extract_datetime as ETL_Run_Datetime,		-- record the datetime when this insert occurred.
			previous_extract_datetime,    	-- record the prior extract datetime and the new extract datetime.
			ID,								-- these remaining fields are exact matchs of Pacifica schema.  We are relying on the Modified datetime to idenfiy a record change.
			AccountID ,
			Created ,
			Modified ,
			Deleted ,
			Name,
			Description,
			Type,
			HasPassingScore,
			PassingScore,
            AdditionalAttemptsAllowed,
			CanReviewPreviousSlides
			
		FROM Quiz  
        WHERE Modified > Previous_Extract_Datetime    ;


	-- If Insert succeeded, then update the control table for this Quiz.
		Update Control_Change_Capture 
		Set Last_Extract_Datetime = New_Extract_Datetime
		where table_name = "Quiz" ; 


COMMIT;

END$$
DELIMITER ;




-- End of script