use pacifica; 

DROP PROCEDURE IF EXISTS Product_StarSchema_ETL;


DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Product_StarSchema_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
BEGIN


/********** BEGIN OF STORED PROCEDURE ***********/

/*
Source is cc_Product, where the mtype and Cmodule data were joined for unqiue pairings of ProductID and AccountID values.
Target is Product_Dim

Product Dim is unique in that it is populated from MType.   There is no Change Capture E/L process that proceeds this specifically for Product.

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
	Set ETL_Datetime = (Select max(ETL_Run_Datetime) from cc_Product ); 
    Set SP_Result = 0;




/****************************  DROP / REFRESH PROCESS   **********************************/

	Truncate Table Product_Dim;

	Insert Into Product_Dim
	(
	  Product_ID,
	  Product_Name,
	  Product_Description,
	  Effective_Datetime,
      Active_Flag
	)
	Select
	  ID ,
	  Name ,
	  Description,
	  ETL_Datetime,
      1 as Active_Flag
	From cc_product ;





COMMIT;

END$$
DELIMITER ;



-- End of script

