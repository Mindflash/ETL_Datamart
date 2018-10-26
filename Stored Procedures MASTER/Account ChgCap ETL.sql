
use pacifica ; 

-- Change Capture E/L    For Accounts

/*


*/


/************ Stored Proc for ETL  **********************/

DROP PROCEDURE IF EXISTS Account_ChgCap_ETL;

DELIMITER $$
CREATE DEFINER=`sa`@`%` PROCEDURE `Account_ChgCap_ETL`(OUT SP_Result INT, OUT SP_MESSAGE varchar(150))
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
	SET previous_extract_datetime =  (select last_extract_datetime from Control_Change_Capture where table_name = "Account")  ; 
	SET sp_result = 0;

-- MAIN PROCESS

	-- Truncate CC_Account
	Truncate Table CC_Account; 

	-- Insert to Change Capture (CC) Staging Table
		INSERT INTO CC_ACCOUNT
		SELECT   
			new_extract_datetime as ETL_Run_Datetime,		-- record the datetime when this insert occurred.
			previous_extract_datetime,    	-- record the prior extract datetime and the new extract datetime.
			ID,								-- these remaining fields are exact matchs of Pacifica schema.  We are relying on the Modified datetime to idenfiy a record change.
			Created,
			Modified,
			Deleted,
			Name,
			Description,
			SalesforceEnabled,
			SalesforceAccountId,
			OwnerEmail,
			Disabled,
			CampaignID,
			SalesforceSyncRequired,
			PricingType,
			TrialExpiration,
			Status,
			StatusModified,
			BillingAccountID,
			PricingMaxTrainees,
			BillingSubscriptionID,
			PricingGroupCode,
			BillingEmail,
			BillingAlternateEmail,
			CancellationDate,
			APIKeyHash,
			APIEnabled,
			InactivityWarningSent,
			CustomUserFieldName,
			ProductTestSuiteEntryID,
			PromoCode,
			InactivityFinalWarningSent,
			PhoneNumber,
			UseNewDeletionRules,
			YammerEnabled,
			YammerAllowedExternalNetwork,
			YammerMPRegEnabled,
			YammerUseStaging,
			HasYammerUsers,
			BillingIntervalType,
			BillingSyncRequired,
			TierID,
			GoodDataFilterUri,
			MaxTeamMembers,
			AdvancedTierTrialExpiration,
			TrialTierID,
			HideAdvancedTierTrial,
			LoginMode,
			GracePeriodDays,
			SAMLEnabled,
			PreviousTrials,
			CustomBrandingEnabled,
			HideMindflashBranding,
			ShopifyShopName,
			ShopifyAuthToken,
			GoodDataCustomDashboardUrl,
			GoodDataCustomCModuleReportUrlTemplate,
			GoodDataCustomSeriesReportUrlTemplate,
			GoodDataCustomCModuleDetailsDashboardTabID,
			GoodDataCustomSeriesDetailsDashboardTabID,
			GoodDataCustomManageTraineesDashboardTabID,
			SalesforceAppEnabled,
			SalesforceAppOrgID,
			SalesforceAppRefreshToken,
			SalesforceAppLastSynced,
			SalesforceAppSandbox,
			SalesforceAppVersion,
			HideFromSearchEngines,
			GoodDataProjectId,
			GoodDataProjectLinkId,
			MindflashGoodDataProjectId,
			DefaultTimezone,
			DemoTrial,
			SMTPEnvelopeFrom,
			DisableTraineeEmails,
			CustomField0,
			Migrated,
			MaintenanceMode,
			Optin,
			GoodDataCustomCourseReportUrlTemplate,
			GoodDataCustomCourseDetailsDashboardTabID,
			GoodDataIntegrationState
		FROM account
		WHERE Modified > Previous_Extract_Datetime   ;     -- remove limit after testing


	-- If Insert succeeded, then update the control table for this account.
		Update Control_Change_Capture 
		Set Last_Extract_Datetime = New_Extract_Datetime
		where table_name = "Account" ; 


COMMIT;

END$$
DELIMITER ;




-- End of script