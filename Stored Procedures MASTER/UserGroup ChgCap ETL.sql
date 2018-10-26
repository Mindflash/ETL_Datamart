
use pacifica ; 







/************ Stored Proc for ETL  **********************/

DROP PROCEDURE IF EXISTS UserGroup_ChgCap_ETL;

DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `UserGroup_ChgCap_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
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
	SET previous_extract_datetime =  (select last_extract_datetime from Control_Change_Capture where table_name = "UserGroup")  ; 
	SET sp_result = 0;

-- MAIN PROCESS

	-- Truncate CC_UserGroup
	Truncate Table CC_UserGroup; 

	-- Insert to Change Capture (CC) Staging Table
		INSERT INTO CC_UserGroup
        (
			ETL_Run_Datetime,
			Last_Extract_Datetime,
			UserGroupUserID,
			UserID,
			AccountID,
			Created,
			Modified,
			Deleted,
			Name 
		)
		SELECT   Distinct
			new_extract_datetime as ETL_Run_Datetime,		-- record the datetime when this insert occurred.
			previous_extract_datetime,    	-- record the prior extract datetime and the new extract datetime.
			ugu.id, 
            ugu.userid,  
            ugu.accountid, 
            ugu.created, 
            ugu.modified, 
            ugu.deleted,
            ug.name
			from usergroupuser ugu
			inner join usergroup ug
			on ugu.usergroupid = ug.id
			WHERE ugu.Modified > Previous_Extract_Datetime   ;


	-- If Insert succeeded, then update the control table for this UserGroup.
		Update Control_Change_Capture 
		Set Last_Extract_Datetime = New_Extract_Datetime
		where table_name = "UserGroup" ; 


COMMIT;

END$$
DELIMITER ;

 

 




-- End of script