
use pacifica ; 

-- Change Capture E/L    For Module_Grade_Status Dim 

/*

MType is not actually a change capture E/L.  If data changes it replaces reference data for that ID for all history.

Since it is such a small table we will just do a drop/refresh each cycle to catch any possible changes.

Can assume the application does not allow for removal of existing code pairs.

*/






/************ Stored Proc for ETL  **********************/

DROP PROCEDURE IF EXISTS Module_Grade_Status_ChgCap_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Module_Grade_Status_ChgCap_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
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
	Set SP_Result = 0;
    
-- This is a drop/refresh process
	Truncate Table cc_Module_Grade_Status ; 

-- Insert to Change Capture (CC) Staging Table
	INSERT INTO CC_Module_Grade_Status
    (
	  ETL_Run_Datetime,
	  ID,  -- MTypeID for Module_Grade_Statuss
	  Name,   -- MTypeName
	  Description
	)
  
    SELECT Distinct
    now() as ETL_Run_Datetime,
	m.ID,
    m.Name,
    m.Description
    
	FROM MType m
    Inner Join CModuleState cs
    On cs.GradeStatus = m.Id  Limit 1000 ; 
 
    
-- If Insert succeeded, then update the control table for this account.  In this case the record is only used for reference.
	Update Control_Change_Capture 
    Set Last_Extract_Datetime = now()   
    where table_name = "Module_Grade_Status" ; 
    

COMMIT;

END$$
DELIMITER ;






-- End of script