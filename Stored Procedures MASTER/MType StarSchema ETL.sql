use pacifica;


/*
Source is cc_mtype
Target is a working table called Mtype_Ref

Goal is to perform a drop refresh for this reference data.

*/







/*********** Create Stored Procedure  *************/

DROP PROCEDURE IF EXISTS MType_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `MType_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN

		
	-- SQLEXCEPTION DECLARATION
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN	
			-- ERROR
            SET SP_Result = 1 ;
            SET SP_MESSAGE = 'An error has occurred, operation rollbacked and the stored procedure was terminated';
			ROLLBACK;
		END;
        

	-- Process Drop Refresh
	Truncate Table mtype_ref;
    
	Insert into mtype_ref
	Select * from cc_mtype ; 

   

COMMIT;

END$$
DELIMITER ;




-- Call 
Call MType_StarSchema_ETL (@STATE, @MESSAGE); 


-- test
Select * from mtype_ref ; 


-- End of script

