 

-- Call example
CALL Account_ChgCap_ETL (@STATE, @MESSAGE); 

SELECT @STATE;
SELECT @MESSAGE ; 






-- SP TEMPLATE

DROP PROCEDURE IF EXISTS name_of_proc ;

DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `name_of_proc`(OUT SP_Result INT, OUT SP_MESSAGE varchar(50))
BEGIN


-- DECLARATIONS

	-- ** procedure specific variables **
	DECLARE new_extract_datetime datetime; 
	DECLARE previous_extract_datetime datetime;
		
        
	-- SQLEXCEPTION DECLARATION
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN	
			-- ERROR
            SET SP_Result = 1 ;
            SET SP_MESSAGE = "SQL EXCEPTION ERROR";
			ROLLBACK;
		END;
		
   
-- START TRANSACTION    
	START TRANSACTION;


-- SET VARIABLES
	SET sp_result = 0;
    
    -- ** Procedure specific sets **
	SET new_extract_datetime = now(); 



-- MAIN PROCESS


	-- ** DO STUFF **



COMMIT;

END$$
DELIMITER ;