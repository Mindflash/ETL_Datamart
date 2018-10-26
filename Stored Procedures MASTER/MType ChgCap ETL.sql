
use pacifica ; 

-- Change Capture E/L    For MType Reference data

/*

MType is not actually a change capture E/L.  If data changes it replaces reference data for that ID for all history.alter

Since it is such a small table we will just do a drop/refresh each cycle to catch any possible changes.

*/








/************ Stored Proc for ETL  **********************/

DROP PROCEDURE IF EXISTS MType_ChgCap_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `MType_ChgCap_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN


-- DECLARATIONS
	
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

-- SET
	Set sp_result = 0;
    
-- This is a drop/refresh process
	Truncate Table cc_Mtype ; 

-- Insert to Change Capture (CC) Staging Table
	INSERT INTO CC_MType
    SELECT 
    now() as ETL_Run_Datetime,
	ID,
    Name,
    Description,
    MTypeGroupID,
    DisplayOrder
	FROM MType ; 
 
    
-- If Insert succeeded, then update the control table for this account.  In this case the record is only used for reference.
	Update Control_Change_Capture 
    Set Last_Extract_Datetime = now()   
    where table_name = "MType" ; 


COMMIT;

END$$
DELIMITER ;


/************ END Stored Proc for ETL  **********************/






-- End of script