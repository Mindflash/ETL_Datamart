
use pacifica ; 

-- Change Capture E/L    For Course

/*
Design objective is to extract data since the last control date with the lowest performance impact on the product table possible.

*/



 



/************ Stored Proc for ETL  **********************/

DROP PROCEDURE IF EXISTS Course_ChgCap_ETL;
DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Course_ChgCap_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
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
	SET previous_extract_datetime =  (select last_extract_datetime from Control_Change_Capture where table_name = "Course")  ; 
	SET sp_result = 0;

-- MAIN PROCESS

	-- Truncate CC_Course
	Truncate Table CC_Course; 

	-- Insert to Change Capture (CC) Staging Table
		INSERT INTO CC_Course
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
			ReEnrollEnabled,
			QuizScoresMatter,
			CanContactTrainer,
			MinimumTimeInCourse,
			IsCoursePageActive,
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
			CourseModified,
			ContentModified,
			CompletionDeadlineLength,
			CompletionDeadlineTimeUnit,
			RefID,
			RefPermissionType,
			PassingScore,
			HasPassingScore,
			CModuleOrderEnforced,
			CModuleFailureEnforced,
			CModuleCompletionEmail,
			YammerSharesEnabled,
			CompositeThumbContentID,
			CompositeThumbWebContentID,
			ThumbUrl,
			SmallThumbUrl,
			LastRemindAllNotStarted,
			LastRemindAllStarted,
			RemindAllNotStartedYammerToken,
			RemindAllStartedYammerToken,
			RemindAllNotStartedUserID,
			RemindAllStartedUserID,
			TrainerImageUrl,
			Language,
			SalesforceAppCourseID,
			SalesforceAppLastSync,
			AllowModuleRetake,
			AllowFastForward,
			UsePreviousScores,
			PrePacificaCourseID
			
		FROM Course  
        WHERE Modified > Previous_Extract_Datetime    ;


	-- If Insert succeeded, then update the control table for this Course.
		Update Control_Change_Capture 
		Set Last_Extract_Datetime = New_Extract_Datetime
		where table_name = "Course" ; 


COMMIT;

END$$
DELIMITER ;




-- End of script