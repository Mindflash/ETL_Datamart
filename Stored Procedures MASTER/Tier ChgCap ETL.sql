
use pacifica ; 

-- Change Capture E/L    For Tier Reference data



/************ Stored Proc for ETL  **********************/

drop procedure if exists tier_ChgCap_ETL;

DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `tier_ChgCap_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN

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
    
-- DECLARE
	Set sp_result = 0;
    
    
-- This is a drop/refresh process
	Truncate Table cc_tier ; 

-- Insert to Change Capture (CC) Staging Table
	INSERT INTO CC_tier
    SELECT 
    now() as ETL_Run_Datetime,
	ID,
    Name,
    Description
	FROM tier ; 
 
    
-- If Insert succeeded, then update the control table for this account.  In this case the record is only used for reference.
	Update Control_Change_Capture 
    Set Last_Extract_Datetime = now()   
    where table_name = "tier" ; 

COMMIT;

END$$
DELIMITER ;



-- End of script