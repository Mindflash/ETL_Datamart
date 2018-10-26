
use pacifica ; 

-- Change Capture E/L    For Module

/*
Design objective is to extract data since the last control date with the lowest performance impact on the product table possible.

*/



/************ Stored Proc for ETL  **********************/

DROP PROCEDURE IF EXISTS CModule_ChgCap_ETL;
DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `CModule_ChgCap_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
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
	SET previous_extract_datetime =  (select last_extract_datetime from Control_Change_Capture where table_name = "CModule")  ; 
	SET sp_result = 0;

-- MAIN PROCESS

	-- Truncate CC_Module
	Truncate Table CC_CModule; 

	-- Insert to Change Capture (CC) Staging Table
		INSERT INTO CC_CModule
		SELECT   
			new_extract_datetime as ETL_Run_Datetime,		-- record the datetime when this insert occurred.
			previous_extract_datetime,    	-- record the prior extract datetime and the new extract datetime.
			ID,
			AccountID,
			Created,
			Modified,
			Deleted,
			OwnerID,
			Type,
			Name,
			Description,
			Status,
			TrainerName,
			TrainerEmail,
			TrainerImageID,
			FrameTransitionType,
			ReEnrollEnabled,
			QuizScoresMatter,
			CanContactTrainer,
			MinimumTimeInCModule,
			IsCModulePageActive,
			AllowTraineesToUnEnroll,
			EnrollmentExpirationDate,
			LastAccessed,
			IsEnrollmentExpirationActive,
			IsShowScoreOnCompletion,
			ReminderActive,
			ReminderInterval,
			StartBy,
			StartByDefined,
			ReTakeEnabled,
			CertificateStatus,
			CertificateTemplate,
			CertificateQuizStatus,
			AccessType,
			TrainerNotificationDate,
			TrainerNotificationEmailsPeriod,
			CModuleModified,
			ContentModified,
			CompletionDeadlineLength,
			CompletionDeadlineTimeUnit,
			RefID,
			RefPermissionType,
			Product,
			Price,
			PassingScore,
			HasPassingScore,
			SeriesType,
			CopiedFromCModuleID,
			CopiedFromSampleCModuleID,
			ShowInMarketplaceCatalog,
			BannedFromMarkeplace,
			YammerSharesEnabled,
			CompositeThumbContentID,
			CompositeThumbWebContentID,
			ThumbUrl,
			CoverContentId,
			MobileConversionState,
			SmallThumbUrl,
			LastRemindAllNotStarted,
			LastRemindAllStarted,
			RemindAllNotStartedYammerToken,
			RemindAllStartedYammerToken,
			RemindAllNotStartedUserID,
			RemindAllStartedUserID,
			SensionUsage,
			Activated,
			TrainerImageUrl,
			Language,
			DisableFastForward,
			SalesforceAppCModuleID,
			SalesforceAppLastSync,
			PacificaCourseID
		
		FROM CModule  
        WHERE Modified > Previous_Extract_Datetime   ;


	-- If Insert succeeded, then update the control table for this Module.
		Update Control_Change_Capture 
		Set Last_Extract_Datetime = New_Extract_Datetime
		where table_name = "CModule" ; 


COMMIT;

END$$
DELIMITER ;







-- End of script