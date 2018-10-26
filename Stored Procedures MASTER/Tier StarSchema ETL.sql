use pacifica;


/*
Source is cc_tier
Target is a working table called Tier_Ref

Goal is to perform a drop refresh for this reference data.

*/







/*********** Create Stored Procedure  *************/

DROP PROCEDURE IF EXISTS Tier_StarSchema_ETL;
DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Tier_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN
	
    -- SQLEXCEPTION DECLARATION
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN	
			-- ERROR
            SET SP_Result = 1 ;
            SET SP_MESSAGE = 'An error has occurred, operation rollbacked and the stored procedure was terminated';
			ROLLBACK;
		END;
    
    
    -- Set sp_result
    Set sp_result = 0 ;
    
    
    -- Process Drop / Refresh 
	Truncate Table tier_ref;
    
	Insert into tier_ref
	Select * from cc_tier ; 
    
    


COMMIT;

END$$
DELIMITER ;

 


-- End of script

