use pacifica; 

DROP PROCEDURE IF EXISTS Course_Grade_Status_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Course_Grade_Status_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN


/********** BEGIN OF STORED PROCEDURE ***********/

/*
Source is cc_Course_Grade_Status, where the mtype and Cmodule data were joined for unqiue pairings of Course_Grade_StatusID and AccountID values.
Target is Course_Grade_Status_Dim

Course_Grade_Status Dim is unique in that it is populated from MType.   There is no Change Capture E/L process that proceeds this specifically for Course_Grade_Status.

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
	Set ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_Course_Grade_Status ); 
    Set SP_Result = 0;




/****************************  DROP / REFRESH PROCESS   **********************************/

	Truncate Table Course_Grade_Status_Dim;

	Insert Into Course_Grade_Status_Dim
	(
	  Course_Grade_Status_ID,
	  Course_Grade_Status_Name,
	  Effective_Datetime,
      Active_Flag
	)
	Select
	  ID ,
	  Name ,
	  ETL_Datetime, 
      1 as Active_Flag
	From cc_Course_Grade_Status ;





COMMIT;

END$$
DELIMITER ;



-- End of script

