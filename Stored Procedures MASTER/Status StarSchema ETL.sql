use pacifica; 

DROP PROCEDURE IF EXISTS Status_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Status_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN


/********** BEGIN OF STORED PROCEDURE ***********/

/*
Source is cc_CStatus, where the mtype and Cmodule data were joined for unqiue pairings of CStatusID and AccountID values.
Target is CStatus_Dim

CStatus Dim is unique in that it is populated from MType.   There is no Change Capture E/L process that proceeds this specifically for CStatus.

*/







-- DECLARATIONS
    
	-- Use Effective_Datetime to ensure a single value is used for all records.
	Declare ETL_Datetime datetime;
        
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
	Set ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_Status ); 
    Set SP_Result = 0;




/****************************  DROP / REFRESH PROCESS   **********************************/

	Truncate Table Status_Dim;

	Insert Into Status_Dim
	(
	  Status_ID,
	  Status_Name,
	  Effective_Datetime,
      Active_Flag
	)
	Select
	  ID ,
	  Name ,
	  ETL_Datetime,
      1 as Active_Flag
	From cc_Status ;



COMMIT;

END$$
DELIMITER ;



-- End of script

