



/*************************   CHANGE CAPTURE ETL DDL , ALL TABLES   *********************/


drop table if exists control_change_capture ; 

CREATE TABLE `control_change_capture` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `Table_Name` varchar(25) DEFAULT NULL,
  `Last_Extract_Datetime` datetime DEFAULT NULL ,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4;



Drop Table cc_account ; 

CREATE TABLE `cc_account` (
   ETL_Run_Datetime datetime,
   Last_Extract_Datetime datetime, 
  `ID` bigint(20) NOT NULL,
  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL,
  `Deleted` datetime DEFAULT NULL,
  `Name` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Description` varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `SalesforceEnabled` datetime DEFAULT NULL,
  `SalesforceAccountId` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `OwnerEmail` varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `Disabled` datetime DEFAULT NULL,
  `CampaignID` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `SalesforceSyncRequired` tinyint(1) NOT NULL DEFAULT '0',
  `PricingType` int(11) NOT NULL DEFAULT '126000100',
  `TrialExpiration` datetime DEFAULT NULL,
  `Status` int(11) NOT NULL DEFAULT '127000100',
  `StatusModified` datetime NOT NULL,
  `BillingAccountID` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `PricingMaxTrainees` int(11) DEFAULT NULL,
  `BillingSubscriptionID` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `PricingGroupCode` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `BillingEmail` varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `BillingAlternateEmail` varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CancellationDate` datetime DEFAULT NULL,
  `APIKeyHash` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `APIEnabled` datetime DEFAULT NULL,
  `InactivityWarningSent` datetime DEFAULT NULL,
  `CustomUserFieldName` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `ProductTestSuiteEntryID` int(11) DEFAULT NULL,
  `PromoCode` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `InactivityFinalWarningSent` datetime DEFAULT NULL,
  `PhoneNumber` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `UseNewDeletionRules` tinyint(1) NOT NULL DEFAULT '0',
  `YammerEnabled` datetime DEFAULT NULL,
  `YammerAllowedExternalNetwork` varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `YammerMPRegEnabled` datetime DEFAULT NULL,
  `YammerUseStaging` tinyint(1) NOT NULL DEFAULT '0',
  `HasYammerUsers` tinyint(1) NOT NULL DEFAULT '0',
  `BillingIntervalType` int(11) NOT NULL DEFAULT '0',
  `BillingSyncRequired` tinyint(1) NOT NULL DEFAULT '0',
  `TierID` bigint(20) NOT NULL DEFAULT '500001',
  `GoodDataFilterUri` varchar(300) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `MaxTeamMembers` int(11) NOT NULL,
  `AdvancedTierTrialExpiration` datetime DEFAULT NULL,
  `TrialTierID` bigint(20) DEFAULT NULL,
  `HideAdvancedTierTrial` datetime DEFAULT NULL,
  `LoginMode` int(11) NOT NULL DEFAULT '172000100',
  `GracePeriodDays` int(11) NOT NULL DEFAULT '0',
  `SAMLEnabled` datetime DEFAULT NULL,
  `PreviousTrials` int(11) DEFAULT NULL,
  `CustomBrandingEnabled` tinyint(1) NOT NULL DEFAULT '0',
  `HideMindflashBranding` tinyint(1) NOT NULL DEFAULT '0',
  `ShopifyShopName` varchar(300) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `ShopifyAuthToken` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `GoodDataCustomDashboardUrl` varchar(250) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `GoodDataCustomCModuleReportUrlTemplate` varchar(250) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `GoodDataCustomSeriesReportUrlTemplate` varchar(250) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `GoodDataCustomCModuleDetailsDashboardTabID` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `GoodDataCustomSeriesDetailsDashboardTabID` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `GoodDataCustomManageTraineesDashboardTabID` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `SalesforceAppEnabled` datetime DEFAULT NULL,
  `SalesforceAppOrgID` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `SalesforceAppRefreshToken` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `SalesforceAppLastSynced` datetime DEFAULT NULL,
  `SalesforceAppSandbox` tinyint(1) NOT NULL DEFAULT '0',
  `SalesforceAppVersion` varchar(10) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `HideFromSearchEngines` tinyint(1) NOT NULL DEFAULT '0',
  `GoodDataProjectId` varchar(45) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `GoodDataProjectLinkId` varchar(45) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `MindflashGoodDataProjectId` varchar(45) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `DefaultTimezone` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `DemoTrial` bigint(20) DEFAULT NULL,
  `SMTPEnvelopeFrom` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `DisableTraineeEmails` tinyint(1) NOT NULL DEFAULT '0',
  `CustomField0` varchar(200) DEFAULT NULL,
  `Migrated` tinyint(1) NOT NULL DEFAULT '0',
  `MaintenanceMode` tinyint(1) DEFAULT '0',
  `Optin` tinyint(1) NOT NULL DEFAULT '0',
  `GoodDataCustomCourseReportUrlTemplate` varchar(250) DEFAULT NULL,
  `GoodDataCustomCourseDetailsDashboardTabID` varchar(100) DEFAULT NULL,
  `GoodDataIntegrationState` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;








Drop Table If Exists cc_Answer ; 

CREATE TABLE cc_Answer (
  ETL_Run_Datetime datetime,
  Last_Extract_Datetime datetime, 

  `AccountID` bigint(20) NOT NULL,
  `QuizID` bigint(20) NOT NULL,  
  `QuestionID` bigint(20) NOT NULL,
  `IndexID` bigint(20) NOT NULL,     
  `AnswerID` bigint(20) NOT NULL,     

  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL,
  `Deleted` datetime DEFAULT NULL,
  
  `QuestionType` varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
   CorrectAnswerFlag tinyint(1), 
   AnswerValue  varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
   CombinationAnswer  varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
   SequenceAnswer  int
   
) ENGINE=InnoDB DEFAULT CHARSET=utf8;





Drop Table If Exists cc_CModule ; 

CREATE TABLE cc_CModule (
  ETL_Run_Datetime datetime,
  Last_Extract_Datetime datetime, 
  `ID` bigint(20) NOT NULL,
  `AccountID` bigint(20) NOT NULL,
  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL,
  `Deleted` datetime DEFAULT NULL,
  `OwnerID` bigint(20) DEFAULT NULL,
  `Type` int(11) NOT NULL,
  `Name` varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Description` varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `Status` int(11) NOT NULL,
  `TrainerName` varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `TrainerEmail` varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `TrainerImageID` bigint(20) DEFAULT NULL,
  `FrameTransitionType` int(11) NOT NULL,
  `ReEnrollEnabled` tinyint(1) NOT NULL DEFAULT '0',
  `QuizScoresMatter` tinyint(1) NOT NULL DEFAULT '1',
  `CanContactTrainer` tinyint(1) NOT NULL DEFAULT '0',
  `MinimumTimeInCModule` int(11) NOT NULL,
  `IsCModulePageActive` tinyint(1) NOT NULL DEFAULT '1',
  `AllowTraineesToUnEnroll` tinyint(1) DEFAULT '1',
  `EnrollmentExpirationDate` datetime DEFAULT NULL,
  `LastAccessed` datetime DEFAULT NULL,
  `IsEnrollmentExpirationActive` tinyint(1) NOT NULL DEFAULT '0',
  `IsShowScoreOnCompletion` tinyint(1) NOT NULL DEFAULT '0',
  `ReminderActive` tinyint(1) NOT NULL DEFAULT '0',
  `ReminderInterval` int(11) DEFAULT '0',
  `StartBy` datetime DEFAULT NULL,
  `StartByDefined` tinyint(1) NOT NULL DEFAULT '0',
  `ReTakeEnabled` tinyint(1) NOT NULL DEFAULT '0',
  `CertificateStatus` int(11) NOT NULL DEFAULT '0',
  `CertificateTemplate` int(11) NOT NULL DEFAULT '0',
  `CertificateQuizStatus` int(11) NOT NULL DEFAULT '0',
  `AccessType` int(11) NOT NULL DEFAULT '0',
  `TrainerNotificationDate` datetime DEFAULT NULL,
  `TrainerNotificationEmailsPeriod` int(11) DEFAULT '0',
  `CModuleModified` datetime DEFAULT NULL,
  `ContentModified` datetime DEFAULT NULL,
  `CompletionDeadlineLength` int(11) NOT NULL DEFAULT '0',
  `CompletionDeadlineTimeUnit` int(11) NOT NULL DEFAULT '0',
  `RefID` bigint(20) DEFAULT NULL,
  `RefPermissionType` int(11) NOT NULL DEFAULT '0',
  `Product` int(11) NOT NULL DEFAULT '140000100',
  `Price` decimal(10,2) DEFAULT NULL,
  `PassingScore` int(11) NOT NULL DEFAULT '60',
  `HasPassingScore` tinyint(1) NOT NULL DEFAULT '0',
  `SeriesType` int(11) NOT NULL DEFAULT '0',
  `CopiedFromCModuleID` bigint(20) DEFAULT NULL,
  `CopiedFromSampleCModuleID` bigint(20) DEFAULT NULL,
  `ShowInMarketplaceCatalog` tinyint(1) DEFAULT '0',
  `BannedFromMarkeplace` datetime DEFAULT NULL,
  `YammerSharesEnabled` tinyint(1) NOT NULL DEFAULT '0',
  `CompositeThumbContentID` bigint(20) DEFAULT NULL,
  `CompositeThumbWebContentID` bigint(20) DEFAULT NULL,
  `ThumbUrl` varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CoverContentId` bigint(20) DEFAULT NULL,
  `MobileConversionState` int(11) NOT NULL DEFAULT '164000100',
  `SmallThumbUrl` varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `LastRemindAllNotStarted` datetime DEFAULT NULL,
  `LastRemindAllStarted` datetime DEFAULT NULL,
  `RemindAllNotStartedYammerToken` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `RemindAllStartedYammerToken` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `RemindAllNotStartedUserID` bigint(20) DEFAULT NULL,
  `RemindAllStartedUserID` bigint(20) DEFAULT NULL,
  `SensionUsage` int(11) NOT NULL DEFAULT '0',
  `Activated` datetime DEFAULT NULL,
  `TrainerImageUrl` varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `Language` varchar(5) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT 'en',
  `DisableFastForward` tinyint(1) NOT NULL DEFAULT '0',
  `SalesforceAppCModuleID` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `SalesforceAppLastSync` datetime DEFAULT NULL,
  `PacificaCourseID` bigint(20) DEFAULT NULL
  
) ENGINE=InnoDB DEFAULT CHARSET=utf8;







Drop Table If Exists cc_CModuleState ; 

CREATE TABLE cc_CModuleState (
  ETL_Run_Datetime datetime,
  Last_Extract_Datetime datetime, 

  `ID` bigint(20) NOT NULL,
  `AccountID` bigint(20) NOT NULL,
  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL,
  `Deleted` datetime DEFAULT NULL,
  `CModuleID` bigint(20) NOT NULL,
  `UserID` bigint(20) NOT NULL,
  `CourseID` bigint(20) NOT NULL DEFAULT '0',
  `Status` int(11) NOT NULL,
  `Invited` datetime DEFAULT NULL,
  `InvitationEmailSent` datetime DEFAULT NULL,
  `InvitationEmailRespondedDate` datetime DEFAULT NULL,
  `ReminderEmailSent` datetime DEFAULT NULL,
  `Started` datetime DEFAULT NULL,
  `Completed` datetime DEFAULT NULL,
  `FinalGrade` int(11) DEFAULT NULL,
  `CompletionEmailSent` datetime DEFAULT NULL,
  `UnInvited` datetime DEFAULT NULL,
  `RetakeIndex` int(11) NOT NULL DEFAULT '0',
  `PokeReminderEmailSent` datetime DEFAULT NULL,
  `GradeStatus` int(11) NOT NULL DEFAULT '0',
  `Required` tinyint(1) NOT NULL DEFAULT '0',
  `LastActivity` datetime DEFAULT NULL,
  `InvitedByUserID` bigint(20) DEFAULT NULL,
  `UnInvitedByUserID` bigint(20) DEFAULT NULL,
  `CompletionDeadline` datetime DEFAULT NULL,
  `Product` int(11) NOT NULL DEFAULT '140000100',
  `SequenceNumber` int(11) DEFAULT NULL,
  `UseSequence` tinyint(1) NOT NULL DEFAULT '0',
  `YammerUserReferenceID` bigint(20) DEFAULT NULL,
  `Duration` int(11) DEFAULT '0',
  `Progress` int(11) DEFAULT '0',
  `hadQuiz` tinyint(1) NOT NULL DEFAULT '0',
  `SalesforceAppLastSync` datetime DEFAULT NULL,
  `ResetViaUserID` bigint(20) DEFAULT NULL,
  `ReuseScore` tinyint(1) DEFAULT '0'

) ENGINE=InnoDB DEFAULT CHARSET=utf8;







Drop Table If Exists cc_course ; 

CREATE TABLE cc_course (
ETL_Run_Datetime datetime,
Last_Extract_Datetime datetime, 
ID bigint(20) NOT NULL,
AccountID bigint(20) NOT NULL,
Created datetime NOT NULL,
Modified datetime NOT NULL,
Deleted datetime DEFAULT NULL,
OwnerID bigint(20) DEFAULT NULL,
Type int(11) NOT NULL,
Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
Description varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
Status int(11) NOT NULL,
TrainerName varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
TrainerEmail varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
TrainerImageID bigint(20) DEFAULT NULL,
ReEnrollEnabled tinyint(1) NOT NULL DEFAULT '0',
QuizScoresMatter tinyint(1) NOT NULL DEFAULT '1',
CanContactTrainer tinyint(1) NOT NULL DEFAULT '0',
MinimumTimeInCourse int(11) NOT NULL,
IsCoursePageActive tinyint(1) NOT NULL DEFAULT '1',
AllowTraineesToUnEnroll tinyint(1) DEFAULT '1',
EnrollmentExpirationDate datetime DEFAULT NULL,
LastAccessed datetime DEFAULT NULL,
IsEnrollmentExpirationActive tinyint(1) NOT NULL DEFAULT '0',
IsShowScoreOnCompletion tinyint(1) NOT NULL DEFAULT '0',
ReminderActive tinyint(1) NOT NULL DEFAULT '0',
ReminderInterval int(11) DEFAULT '0',
StartBy datetime DEFAULT NULL,
StartByDefined tinyint(1) NOT NULL DEFAULT '0',
ReTakeEnabled tinyint(1) NOT NULL DEFAULT '0',
CertificateStatus int(11) NOT NULL DEFAULT '0',
CertificateTemplate int(11) NOT NULL DEFAULT '0',
CertificateQuizStatus int(11) NOT NULL DEFAULT '0',
AccessType int(11) NOT NULL DEFAULT '0',
TrainerNotificationDate datetime DEFAULT NULL,
TrainerNotificationEmailsPeriod int(11) DEFAULT '0',
CourseModified datetime DEFAULT NULL,
ContentModified datetime DEFAULT NULL,
CompletionDeadlineLength int(11) NOT NULL DEFAULT '0',
CompletionDeadlineTimeUnit int(11) NOT NULL DEFAULT '0',
RefID bigint(20) DEFAULT NULL,
RefPermissionType int(11) NOT NULL DEFAULT '0',
PassingScore int(11) NOT NULL DEFAULT '60',
HasPassingScore tinyint(1) NOT NULL DEFAULT '0',
CModuleOrderEnforced tinyint(1) NOT NULL DEFAULT '0',
CModuleFailureEnforced tinyint(1) NOT NULL DEFAULT '0',
CModuleCompletionEmail tinyint(1) NOT NULL DEFAULT '0',
YammerSharesEnabled tinyint(1) NOT NULL DEFAULT '0',
CompositeThumbContentID bigint(20) DEFAULT NULL,
CompositeThumbWebContentID bigint(20) DEFAULT NULL,
ThumbUrl varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
SmallThumbUrl varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
LastRemindAllNotStarted datetime DEFAULT NULL,
LastRemindAllStarted datetime DEFAULT NULL,
RemindAllNotStartedYammerToken varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
RemindAllStartedYammerToken varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
RemindAllNotStartedUserID bigint(20) DEFAULT NULL,
RemindAllStartedUserID bigint(20) DEFAULT NULL,
TrainerImageUrl varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
Language varchar(5) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT 'en',
SalesforceAppCourseID varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
SalesforceAppLastSync datetime DEFAULT NULL,
AllowModuleRetake tinyint(1) NOT NULL DEFAULT '0',
AllowFastForward tinyint(1) NOT NULL DEFAULT '0',
UsePreviousScores tinyint(1) NOT NULL DEFAULT '0',
PrePacificaCourseID bigint(20) DEFAULT NULL
  
) ENGINE=InnoDB DEFAULT CHARSET=utf8;







 
Drop Table if exists cc_Course_Grade_Status ; 

   
CREATE TABLE `cc_Course_Grade_Status` (
  ETL_Run_Datetime datetime,
  ID  bigint, 
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  Description  varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;








Drop Table If Exists cc_CourseState ; 

CREATE TABLE cc_CourseState (
  ETL_Run_Datetime datetime,
  Last_Extract_Datetime datetime, 

  `ID` bigint(20) NOT NULL,
  `AccountID` bigint(20) NOT NULL,
  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `Deleted` datetime DEFAULT NULL,
  `CourseID` bigint(20) NOT NULL,
  `UserID` bigint(20) NOT NULL,
  `Status` int(11) NOT NULL,
  `Invited` datetime DEFAULT NULL,
  `InvitationEmailSent` datetime DEFAULT NULL,
  `InvitationEmailRespondedDate` datetime DEFAULT NULL,
  `ReminderEmailSent` datetime DEFAULT NULL,
  `Started` datetime DEFAULT NULL,
  `Completed` datetime DEFAULT NULL,
  `FinalGrade` int(11) DEFAULT NULL,
  `CompletionEmailSent` datetime DEFAULT NULL,
  `RetakeIndex` int(11) NOT NULL DEFAULT '0',
  `PokeReminderEmailSent` datetime DEFAULT NULL,
  `GradeStatus` int(11) NOT NULL DEFAULT '0',
  `Required` tinyint(1) NOT NULL DEFAULT '0',
  `LastActivity` datetime DEFAULT NULL,
  `InvitedByUserID` bigint(20) DEFAULT NULL,
  `CompletionDeadline` datetime DEFAULT NULL,
  `UseSequence` tinyint(1) NOT NULL DEFAULT '0',
  `YammerUserReferenceID` bigint(20) DEFAULT NULL,
  `Duration` int(11) DEFAULT '0',
  `Progress` int(11) DEFAULT '0',
  `SalesforceAppLastSync` datetime DEFAULT NULL,
  `ResetViaUserID` bigint(20) DEFAULT NULL

) ENGINE=InnoDB DEFAULT CHARSET=utf8;






Drop Table If Exists cc_FrameState ; 

CREATE TABLE cc_FrameState (
  ETL_Run_Datetime datetime,
  Last_Extract_Datetime datetime, 

  `FrameStateID` bigint(20) NOT NULL,
  `FrameID` bigint(20) NOT NULL,
  `AccountID` bigint(20) NOT NULL,
  `QuizID` bigint(20) NOT NULL,    		-- From Frame Table
  `QuestionID` bigint(20) NOT NULL,   	-- From Frame Table
    
  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL,
  `Deleted` datetime DEFAULT NULL,
  
  `UserID` bigint(20) NOT NULL,     
  `CModuleID` bigint(20) NOT NULL,    
  `CourseID` bigint(20) NOT NULL,

  `FrameType`  int(11) NOT NULL,   		-- From Frame Table
 --   `QuestionType`   varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,   		-- From Frame Table
  
  -- The following are all from the Framestate table
  `Status` int(11) NOT NULL,  
  `QuestionPoints` int(11) NOT NULL DEFAULT '0',
 --  `QuestionAnswer` longblob,
  `QuestionIsCorrect` tinyint(1) NOT NULL DEFAULT '0',
  `QuestionAttempts` int(11) NOT NULL DEFAULT '0',
  `QuestionListRandomized` varchar(10000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `Score` int(11) DEFAULT NULL,
  `GradeStatus` int(11) NOT NULL DEFAULT '0',
  `Product` int(11) NOT NULL DEFAULT '140000100',
  `Duration` int(11) DEFAULT '0',
  `ProgressSeconds` int(11) DEFAULT NULL,
  `EngagementScore` int(11) DEFAULT NULL,
  `ScoProperties` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
   AnswerIndexSelected int(11) DEFAULT NULL,
   EssayAnswerLength  int(11) DEFAULT NULL
   
) ENGINE=InnoDB DEFAULT CHARSET=utf8;





-- DDL  
Drop Table if exists cc_Module_Grade_Status ; 

   
CREATE TABLE `cc_Module_Grade_Status` (
  ETL_Run_Datetime datetime,
  ID  bigint, 
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  Description  varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;






Drop Table cc_mtype ; 

   
CREATE TABLE `cc_mtype` (
   ETL_Run_Datetime datetime,
  `ID` int(11) NOT NULL,
  `Name` varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Description` varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `MTypeGroupID` int(11) NOT NULL,
  `DisplayOrder` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;





Drop Table if exists cc_product ; 

   
CREATE TABLE `cc_product` (
  ETL_Run_Datetime datetime,
  ID  bigint,
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  Description  varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;





Drop Table if exists cc_Question ; 

CREATE TABLE `cc_Question` (
   ETL_Run_Datetime datetime,
   Last_Extract_Datetime datetime, 
  `ID` bigint(20) NOT NULL,
  `AccountID` bigint(20) NOT NULL,
  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL,
  `Deleted` datetime DEFAULT NULL,
  `QuizID` bigint(20) NOT NULL,
  `Type` int(11) NOT NULL,
  `QuestionInfoID` bigint(20) NOT NULL,
  `Title` varchar(5000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `PointValue` int(11) NOT NULL DEFAULT '0',
  `PartialCorrect` tinyint(1) NOT NULL DEFAULT '0',
  `AttemptsAllowed` int(11) NOT NULL DEFAULT '1',
  `DisplayIndex` int(11) NOT NULL DEFAULT '0',
  `IsValid` tinyint(1) NOT NULL DEFAULT '1',
  `Product` int(11) NOT NULL DEFAULT '140000100',
  `Feedback` varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;








Drop Table if exists cc_Quiz ; 

CREATE TABLE `cc_Quiz` (
   ETL_Run_Datetime datetime,
   Last_Extract_Datetime datetime, 
  `ID` bigint(20) NOT NULL,
  `AccountID` bigint(20) NOT NULL,
  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL,
  `Deleted` datetime DEFAULT NULL,
  `Name` varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `Description` varchar(2000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `Type` int(11) NOT NULL DEFAULT '0',
  `HasPassingScore` tinyint(1) NOT NULL DEFAULT '0',
  `PassingScore` int(11) NOT NULL DEFAULT '0',
  `AdditionalAttemptsAllowed` int(11) NOT NULL DEFAULT '0',
  `CanReviewPreviousSlides` tinyint(1) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;





 
Drop Table if exists cc_Status ; 

   
CREATE TABLE `cc_Status` (
  ETL_Run_Datetime datetime,
  ID  bigint, 
  Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  Description  varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;







Drop Table if exists cc_tier ; 

CREATE TABLE cc_tier (
   ETL_Run_Datetime datetime,
   ID int(11) NOT NULL,
   Name  varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
   Description  varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;





Drop Table If Exists cc_TraineeCertificate ; 

CREATE TABLE cc_TraineeCertificate (
  ETL_Run_Datetime datetime,
  Last_Extract_Datetime datetime, 

  `ID` bigint(20) NOT NULL,
  `AccountID` bigint(20) NOT NULL,
  `CourseID` bigint(20) DEFAULT NULL,
  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL,
  `Deleted` datetime DEFAULT NULL,
  `CModuleID` bigint(20) DEFAULT NULL,
  `UserID` bigint(20) NOT NULL,
  `QuizScore` int(11) DEFAULT NULL,
  `CModuleCompleted` datetime NOT NULL

) ENGINE=InnoDB DEFAULT CHARSET=utf8;





Drop Table if exists cc_UserGroup ; 

CREATE TABLE `cc_UserGroup` (
   ETL_Run_Datetime datetime,
   Last_Extract_Datetime datetime, 
  `UserGroupUserID` bigint(20) NOT NULL,            -- UserGroupUser_ID  is the PK.  This will be unique for User and Group combinations.
   `UserID`  bigint(20) NOT NULL,      -- The User ID
  `AccountID` bigint(20) NOT NULL,
  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL,
  `Deleted` datetime DEFAULT NULL,
  `Name` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;






Drop Table if exists cc_UserRecord ; 

CREATE TABLE `cc_UserRecord` (
   ETL_Run_Datetime datetime,
   Last_Extract_Datetime datetime, 
  `ID` bigint(20) NOT NULL,
  `AccountID` bigint(20) NOT NULL,
  `Created` datetime NOT NULL,
  `Modified` datetime NOT NULL,
  `Deleted` datetime DEFAULT NULL,
  `Status` int(11) NOT NULL,
  `UnarchivedStatus` int(11) NOT NULL DEFAULT '0',
  `Name` varchar(300) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `FirstName` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `LastName` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `Email` varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `Username` varchar(254) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `Department` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `TimeZoneType` int(11) NOT NULL DEFAULT '0',
  `SalesforceUserName` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `SalesforceContactId` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `SalesforceUserId` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `ShowWelcomeMat` tinyint(1) DEFAULT '0',
  `SalesforceSyncRequired` tinyint(1) NOT NULL DEFAULT '0',
  `PasswordCrypt` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `ProductMembership` int(11) NOT NULL DEFAULT '16777217',
  `PhoneNumber` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `MasterUserRecordID` bigint(20) DEFAULT NULL,
  `YammerID` int(11) DEFAULT NULL,
  `YammerExternalNetworkID` int(11) DEFAULT NULL,
  `BillingStatus` int(11) NOT NULL DEFAULT '163000100',
  `Permissions` int(11) NOT NULL DEFAULT '169000100',
  `GoodDataUserID` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CustomField0` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CustomField1` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CustomField2` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CustomField3` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CustomField4` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CustomField5` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CustomField6` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CustomField7` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CustomField8` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CustomField9` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `PasswordExpiration` datetime DEFAULT NULL,
  `FederatedId` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `GoodDataAttributeUri` varchar(300) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `SalesforceAppUserId` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `SalesforceAppUserType` int(11) DEFAULT NULL,
  `SalesforceAppLastSync` datetime DEFAULT NULL,
  `KissmetricsAliased` tinyint(1) DEFAULT '0',
  `GoodDataUserEmailHash` varchar(45) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;









Drop Table if Exists account_dim; 
Create Table Account_Dim
(
  ID bigint not null auto_increment,
  Effective_Datetime datetime, 
  Expiration_Datetime datetime, 
  Active_Flag tinyint(1) NOT NULL DEFAULT '0',
  Deleted_Flag tinyint(1) NOT NULL DEFAULT '0',
  Deleted_Datetime datetime, 

  Account_ID bigint, 
  Account_Name varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  Account_Description varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
 
  First_Added_Datetime datetime,   
  Last_Modified_Datetime datetime,
  Salesforce_Enabled_Flag    tinyint(1) NOT NULL DEFAULT '0',
  SalesforceAppEnabled_Datetime   datetime,
  Owner_Email  varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Disabled_Flag  tinyint(1) NOT NULL DEFAULT '0',
  Disabled_Datetime  datetime,
  Pricing_Type  varchar(5) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,     --  (Pay, Trial)
  Trial_Expiration_Datetime datetime,
  Account_Status  varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,    --   (Active, Gace, Grace2, TrialExpired, Cancelled, CancellationPending)

   Account_Status_Modified_Datetime datetime, 
   Cancellation_Datetime 	datetime,
   Pricing_Max_Trainees  int(11) DEFAULT NULL,
   Billing_Email  varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
   Billing_Alternate_Email   varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
   API_Enabled_Flag  tinyint(1) NOT NULL DEFAULT '0',
   API_Enabled_Datetime   datetime,
   Inactivity_Warning_Sent_Datetime  datetime,
   Promo_Code  varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
   Inactivity_Final_Warning_Sent_Datetime  datetime,
   Phone_Number  varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
   Use_New_Deletion_Rules_Flag   tinyint(1) NOT NULL DEFAULT '0',
   Billing_Interval_Type   varchar(15) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,       --  (None, AnnualInvoiced, Monthly, Annual)
   Account_Tier  varchar(10) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,            --  (Basic, Standard, Advanced, Premium, Trial, Enterprise, Pro)

  Max_Team_Members_Number int(11) NOT NULL,
  Advanced_Tier_Trial_Expiration_Datetime  datetime,
  Trial_Tier   varchar(10) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,      
  Login_Mode    varchar(10) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,           -- (Email, Username, None)
  Grace_Period_Days int(11) NOT NULL DEFAULT '0',
  SAMLEnabled_Flag   tinyint(1) NOT NULL DEFAULT '0',
  SAMLEnabled_Datetime   datetime DEFAULT NULL,
  PreviousTrials_Number   int(11) DEFAULT NULL,
  Custom_Branding_Enabled_Flag  tinyint(1) NOT NULL DEFAULT '0',
  Hide_Mindflash_Branding_Flag  tinyint(1) NOT NULL DEFAULT '0',
  Shopify_Shop_Name  varchar(300) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  GDPC_Opt_In_Flag        tinyint(1) NOT NULL DEFAULT '0',
  Maintenance_Mode_Flag   tinyint(1) NOT NULL DEFAULT '0',
  
  -- validation check fields.  not visible to user
  PricingType bigint,
  Status bigint,
  BillingIntervalType bigint,
  LoginMode bigint,
  TierID bigint,
  TrialTierID bigint,
  ETL_Run_Datetime datetime,
  
  PRIMARY KEY (ID) 
  
  ) ;







Drop Table if Exists Answer_Dim; 

Create Table Answer_Dim
(
  ID bigint not null auto_increment,
  
  Effective_Datetime datetime, 
  Expiration_Datetime datetime, 
  Active_Flag tinyint(1) NOT NULL DEFAULT '0',   

  Deleted_Flag tinyint(1) NOT NULL DEFAULT '0', 
  Deleted_Datetime datetime DEFAULT NULL,

  Question_ID bigint(20) NOT NULL,  
  Answer_ID bigint(20) NOT NULL,   
  Account_ID bigint(20) NOT NULL,

  First_Added_Datetime datetime NOT NULL,
  Last_Modified_Datetime datetime NOT NULL,
  
  Answer_Value  varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Correct_Answer_Flag tinyint(1), 
  Combination_Answer_Description  varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Sequence_Answer_Index   varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  
  -- Admin Fields
  Quiz_ID  bigint(20) NOT NULL,  
  Answer_Index_Number  int, 
  Question_Type varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  ETL_Run_Datetime datetime,
  Process_Flag tinyint(1),
  PRIMARY KEY (ID) 
   
) ENGINE=InnoDB DEFAULT CHARSET=utf8;






Drop Table If Exists Answer_Fact ; 

Create Table Answer_Fact
(
AF_ID bigint not null auto_increment,
Framestate_ID bigint(20) NOT NULL,
Deleted_Flag tinyint(1),
Deleted_Datetime datetime,
Effective_Datetime datetime,
Expiration_Datetime datetime,
Active_Flag tinyint(1) ,

-- FKs
Account_ID  bigint(20) NOT NULL, 
Trainee_ID bigint(20) NOT NULL, 
Course_ID  bigint(20) NOT NULL, 
Module_ID bigint(20) NOT NULL, 
Quiz_ID bigint(20) NOT NULL,
Question_ID bigint(20) NOT NULL,
Status_ID   int(11) NOT NULL, 

-- Date / Time fields
First_Added_Date date, 
First_Added_Time time,
First_Added_Datetime datetime,
Last_Modified_Date  Date , 
Last_Modified_Time time,
Last_Modified_Datetime  Datetime ,

Answer_Value  varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
Correct_Answer_Flag tinyint(1) , 
Answer_Index_Selected   int(11) NOT NULL, 
Essay_Answer_Length   int(11) NOT NULL, 

ETL_Run_Datetime datetime,
  
  PRIMARY KEY (AF_ID) 

) ENGINE=InnoDB DEFAULT CHARSET=utf8;





 

Drop Table if Exists Course_Dim; 

Create Table Course_Dim
(
  ID bigint not null auto_increment,
  Effective_Datetime datetime, 
  Expiration_Datetime datetime, 
  Active_Flag tinyint(1) NOT NULL DEFAULT '0',   

  Deleted_Flag tinyint(1) NOT NULL DEFAULT '0', 
  Deleted_Datetime datetime DEFAULT NULL,

  Course_ID bigint(20) NOT NULL,
  Account_ID bigint(20) NOT NULL,

  First_Added_Datetime datetime NOT NULL,
  Last_Modified_Datetime datetime NOT NULL,

  Owner_ID bigint(20) DEFAULT NULL,
  Course_Type   varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,			-- mtype
  Course_Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  Course_Description varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Course_Status  varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,			-- mtype
  Trainer_Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Trainer_Email varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  ReEnroll_Enabled_Flag tinyint(1) NOT NULL DEFAULT '0',
  Quiz_Scores_Matter_Flag tinyint(1) NOT NULL DEFAULT '1',
  Can_Contact_Trainer_Flag tinyint(1) NOT NULL DEFAULT '0',
  Is_Course_Page_Active_Flag tinyint(1) NOT NULL DEFAULT '1',
  Allow_Trainees_To_UnEnroll_Flag tinyint(1) DEFAULT '1',
  Enrollment_Expiration_Datetime datetime DEFAULT NULL,
  Last_Accessed_Datetime datetime DEFAULT NULL,
  Enrollment_Expiration_Active_Flag tinyint(1) NOT NULL DEFAULT '0',
  Show_Score_On_Completion_Flag tinyint(1) NOT NULL DEFAULT '0',
  Reminder_Active_Flag tinyint(1) NOT NULL DEFAULT '0',
  Reminder_Interval  varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,			-- mtype
  Start_By_Date datetime DEFAULT NULL,
  Start_By_Defined_Flag tinyint(1) NOT NULL DEFAULT '0',
  ReTake_Enabled_Flag tinyint(1) NOT NULL DEFAULT '0',
  Certificate_Status  varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,			-- mtype
	Certificate_Template  varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,			-- mtype
	Certificate_Quiz_Status varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,			-- mtype
    Access_Type  varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,			-- mtype
	Trainer_Notification_Datetime datetime DEFAULT NULL,
	Trainer_Notification_Emails_Period   varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,			-- mtype
    Course_Modified_Datetime datetime DEFAULT NULL,
	Content_Modified_Datetime datetime DEFAULT NULL,
	Completion_Deadline_Length int(11) NOT NULL DEFAULT '0',
	-- CompletionDeadlineTimeUnit int(11) NOT NULL DEFAULT '0',
    -- RefID bigint(20) DEFAULT NULL,
	-- RefPermissionType int(11) NOT NULL DEFAULT '0',
	Passing_Score int(11) NOT NULL DEFAULT '60',
	Has_Passing_Score_Flag tinyint(1) NOT NULL DEFAULT '0',
	Module_Order_Enforced_Flag tinyint(1) NOT NULL DEFAULT '0',
	Module_Failure_Enforced_Flag tinyint(1) NOT NULL DEFAULT '0',
	Module_Completion_Email_Flag tinyint(1) NOT NULL DEFAULT '0',
	Yammer_Shares_Enabled_Flag tinyint(1) NOT NULL DEFAULT '0',
    --  CompositeThumbContentID bigint(20) DEFAULT NULL,
	--  CompositeThumbWebContentID bigint(20) DEFAULT NULL,
	--  ThumbUrl varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
	--  SmallThumbUrl varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
	Last_Remind_All_Not_Started_Datetime datetime DEFAULT NULL,
	Last_Remind_All_Started_Datetime datetime DEFAULT NULL,
    -- RemindAllNotStartedYammerToken varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
	-- RemindAllStartedYammerToken varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
	-- RemindAllNotStartedUserID bigint(20) DEFAULT NULL,
	-- RemindAllStartedUserID bigint(20) DEFAULT NULL,
	-- TrainerImageUrl varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
	Language varchar(5) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT 'en',
    -- SalesforceAppCourseID varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
	-- SalesforceAppLastSync datetime DEFAULT NULL,
	Allow_Module_Retake_Flag tinyint(1) NOT NULL DEFAULT '0',
	Allow_Fast_Forward_Flag tinyint(1) NOT NULL DEFAULT '0',
	Use_Previous_Scores_Flag tinyint(1) NOT NULL DEFAULT '0',
	--  PrePacificaCourseID bigint(20) DEFAULT NULL,

  -- Admin 
	Type int(11) NOT NULL,
	Status int(11) NOT NULL,
	ReminderInterval int(11) DEFAULT '0',
	CertificateStatus int(11) NOT NULL DEFAULT '0',
	CertificateTemplate int(11) NOT NULL DEFAULT '0', 
	CertificateQuizStatus int(11) NOT NULL DEFAULT '0',     
	AccessType int(11) NOT NULL DEFAULT '0',
	TrainerNotificationEmailsPeriod int(11) DEFAULT '0' ,
    ETL_Run_Datetime datetime,
  
	PRIMARY KEY (ID) 
       
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    
    
    
    


Drop Table if Exists Course_Grade_Status_Dim; 

Create Table Course_Grade_Status_Dim
(
  Course_Grade_Status_ID  bigint,
  Course_Grade_Status_Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
--   Course_Grade_Status_Description varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  
  Effective_Datetime datetime,
  Expiration_Datetime datetime,
  Active_Flag tinyint(1) default '1'
  
) ;





Drop Table If Exists Module_Participation_Fact ; 

Create Table Module_Participation_Fact

(
MPF_ID bigint not null auto_increment,
CModuleState_ID bigint not null,

Deleted_Flag tinyint(1),
Deleted_Datetime datetime,
Effective_Datetime datetime,
Expiration_Datetime datetime,
Active_Flag int,

-- FKs
Account_ID  bigint,
Trainee_ID bigint,
Invited_By_User_ID bigint,
UnInvited_By_User_ID bigint,
Course_ID  bigint,
Module_ID bigint,
Status_ID int(11) NOT NULL,
Module_Grade_Status_ID bigint,
Product_ID   bigint,

-- Date / Time fields
First_Added_Date date, 
First_Added_Time time,
First_Added_Datetime datetime,

Last_Modified_Date  Date  , 
Last_Modified_Time time ,
Last_Modified_Datetime  Datetime  ,

Module_Started_Date date, 
Module_Started_Time time,
Module_Started_Datetime datetime,

Invited_Date date,  
Invited_Time time,
Invited_Datetime datetime,

Invitation_Email_Sent_Date date, 
Invitation_Email_Sent_Time time,
Invitation_Email_Sent_Datetime datetime,

Invitation_Email_Responded_Date date, 
Invitation_Email_Responded_Time time,
Invitation_Email_Responded_Datetime datetime,

UnInvited_Date date,  
UnInvited_Time time,
UnInvited_Datetime datetime,

Reminder_Email_Sent_Date date, 
Reminder_Email_Sent_Time time ,
Reminder_Email_Sent_Datetime datetime,

Module_Completion_Date date, 
Module_Completion_Time time,
Module_Completion_Datetime datetime,

Completion_Email_Sent_Date date, 
Completion_Email_Sent_Time time,
Completion_Email_Sent_Datetime datetime,

Poke_Reminder_Email_Sent_Date date,
Poke_Reminder_Email_Sent_Time time,
Poke_Reminder_Email_Sent_Datetime datetime,

Trainee_Completion_Deadline_Date date,
Trainee_Completion_Deadline_Time time,
Trainee_Completion_Deadline_Datetime datetime,

Module_Last_Activity_Date date,
Module_Last_Activity_Time time,
Module_Last_Activity_Datetime datetime,

In_Progress_Complete_Percent int ,
Module_Elapsed_Duration_Seconds int,
Module_Final_Grade_Percent int, 
Module_Required_Flag tinyint(1),
Took_Quiz_Flag tinyint(1),

ETL_Run_Datetime datetime,
  
  PRIMARY KEY (MPF_ID) 

) ENGINE=InnoDB DEFAULT CHARSET=utf8;







  
	

Drop Table if Exists Module_Dim; 

Create Table Module_Dim
(
  ID bigint not null auto_increment,
  Effective_Datetime datetime, 
  Expiration_Datetime datetime, 
  Active_Flag tinyint(1) NOT NULL DEFAULT '0',   
  Deleted_Flag tinyint(1) NOT NULL DEFAULT '0', 
  Deleted_Datetime datetime DEFAULT NULL,
  Module_ID bigint(20) NOT NULL,
  Account_ID bigint(20) NOT NULL,
  First_Added_Datetime datetime NOT NULL,
  Last_Modified_Datetime datetime NOT NULL,
  Owner_ID bigint(20) DEFAULT NULL,
  Module_Type varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  Module_Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  Module_Description varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Module_Status varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  Trainer_Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Trainer_Email varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  ReEnroll_Enabled_Flag tinyint(1) NOT NULL DEFAULT '0',
  Quiz_Scores_Matter_Flag tinyint(1) NOT NULL DEFAULT '1',
  Can_Contact_Trainer_Flag tinyint(1) NOT NULL DEFAULT '0',
  Module_Page_Active_Flag Tinyint(1) NOT NULL DEFAULT '1',
  Allow_Trainees_To_UnEnroll_Flag tinyint(1) DEFAULT '1',
  Last_Accessed_Datetime datetime DEFAULT NULL,

  Show_Score_On_Completion_Flag tinyint(1) NOT NULL DEFAULT '0',
  Reminder_Active_Flag tinyint(1) NOT NULL DEFAULT '0',
  Reminder_Interval varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,                     --  mtype
  Start_By_Date datetime DEFAULT NULL,
  Start_By_Defined_Flag tinyint(1) NOT NULL DEFAULT '0',
  ReTake_Enabled_Flag tinyint(1) NOT NULL DEFAULT '0',
  Certificate_Status varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,                -- mtype
  Certificate_Template varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,                -- mtype
  Certificate_Quiz_Status varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,                -- mtype
  Access_Type varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,                -- mtype
  Trainer_Notification_Datetime datetime DEFAULT NULL,
  Trainer_Notification_Emails_Period varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,                -- mtype
  Module_Modified_Datetime datetime DEFAULT NULL,
  Content_Modified_Datetime datetime DEFAULT NULL,
  Completion_Deadline_Length int(11) NOT NULL DEFAULT '0',
  Completion_Deadline_Time_Unit varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,                -- mtype
  Passing_Score int(11) NOT NULL DEFAULT '60',
  Has_Passing_Score_Flag tinyint(1) NOT NULL DEFAULT '0',
  SeriesType int(11) NOT NULL DEFAULT '0',
  Copied_From_Module_Name  varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,    -- lookup
  Copied_From_Module_ID bigint(20) DEFAULT NULL,
  Copied_From_Sample_Module_Name  varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,     -- lookup
  Copied_From_Sample_Module_ID bigint(20) DEFAULT NULL,
  Show_In_Marketplace_Catalog_Flag tinyint(1) DEFAULT '0',
  Banned_From_Markeplace_Datetime datetime DEFAULT NULL,
  Last_Remind_All_Not_Started_Datetime datetime DEFAULT NULL,
  Last_Remind_All_Started_Datetime datetime DEFAULT NULL,
  Language varchar(5) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT 'en',
  Disable_Fast_Forward tinyint(1) NOT NULL DEFAULT '0',
  
  -- Admin
  Type int(11) NOT NULL,
  Status int(11) NOT NULL,
  ReminderInterval int(11) DEFAULT '0', 
  CertificateStatus int(11) NOT NULL DEFAULT '0',                
  CertificateTemplate int(11) NOT NULL DEFAULT '0',
  CertificateQuizStatus int(11) NOT NULL DEFAULT '0',
  AccessType int(11) NOT NULL DEFAULT '0',
  TrainerNotificationEmailsPeriod int(11) DEFAULT '0',
  CompletionDeadlineTimeUnit int(11) NOT NULL DEFAULT '0',
  Product int(11) NOT NULL DEFAULT '140000100', 
  ETL_Run_Datetime datetime,
  
  PRIMARY KEY (ID) 
       
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;











Drop Table if Exists Module_Grade_Status_Dim; 

Create Table Module_Grade_Status_Dim
(
  Module_Grade_Status_ID  bigint,
  Module_Grade_Status_Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
--   Module_Grade_Status_Description varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  
  Effective_Datetime datetime,
  Expiration_Datetime datetime,
  Active_Flag tinyint(1) default '1'
  
) ;






 
 
Drop Table if Exists mtype_ref; 
Create Table mtype_ref
(  ETL_Run_Datetime datetime,
  `ID` int(11) NOT NULL,
  `Name` varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Description` varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `MTypeGroupID` int(11) NOT NULL,
  `DisplayOrder` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 
 



 

Drop Table if Exists Product_Dim; 

Create Table Product_Dim
(
  Product_ID  bigint,
  Product_Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  Product_Description varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  
  Effective_Datetime datetime,
  Expiration_Datetime datetime,
  Active_Flag tinyint(1) default '1'
  
) ;



 

Drop Table if Exists Question_Dim; 

Create Table Question_Dim
(
  ID bigint not null auto_increment,
  Effective_Datetime datetime, 
  Expiration_Datetime datetime, 
  Active_Flag tinyint(1) NOT NULL DEFAULT '0',   

  Deleted_Flag tinyint(1) NOT NULL DEFAULT '0', 
  Deleted_Datetime datetime DEFAULT NULL,

  Question_ID bigint(20) NOT NULL,
  Account_ID bigint(20) NOT NULL,
  Quiz_ID  bigint(20) NOT NULL,
  
  First_Added_Datetime datetime NOT NULL,
  Last_Modified_Datetime datetime NOT NULL,
  
  Question_Title varchar(5000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Question_Type varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  --  `QuestionInfoID` bigint(20) NOT NULL,   -- have not resolved what this ties to
  Point_Value int(11) NOT NULL DEFAULT '0',
  Partial_Correct_Flag tinyint(1) NOT NULL DEFAULT '0',   
  Attempts_Allowed int(11) NOT NULL DEFAULT '1',
  Is_Valid_Flag tinyint(1) NOT NULL DEFAULT '1',
  Product_Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  -- `Feedback` varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  
  -- Admin
  Type int(11) NOT NULL,
  Product int(11) NOT NULL DEFAULT '140000100',
  ETL_Run_Datetime datetime,
  
  PRIMARY KEY (ID) 
  
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

 




Drop Table If Exists Quiz_Participation_Fact ; 

Create Table Quiz_Participation_Fact
(
QPF_ID bigint not null auto_increment,
Framestate_ID bigint(20) NOT NULL,
Quiz_ID bigint(20) NOT NULL,

Deleted_Flag tinyint(1),
Deleted_Datetime datetime,

Effective_Datetime datetime,
Expiration_Datetime datetime,
Active_Flag tinyint(1) ,

-- FKs
Account_ID  bigint(20) NOT NULL, 
Trainee_ID bigint(20) NOT NULL, 
Course_ID  bigint(20) NOT NULL, 
Module_ID bigint(20) NOT NULL, 
Quiz_Grade_Status  int(11) NOT NULL, 
Status_ID   int(11) NOT NULL, 

-- Date / Time fields
First_Added_Date date, 
First_Added_Time time,
First_Added_Datetime datetime,

Last_Modified_Date  Date , 
Last_Modified_Time time,
Last_Modified_Datetime  Datetime ,

Quiz_Score int(11) DEFAULT NULL,

ETL_Run_Datetime datetime,

--  Frame_Type_Desc varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
--  Frametype   int(11) NOT NULL,
  
  PRIMARY KEY (QPF_ID) 

) ENGINE=InnoDB DEFAULT CHARSET=utf8;








Drop Table if Exists Quiz_Dim; 

Create Table Quiz_Dim
(
  ID bigint not null auto_increment,
  Effective_Datetime datetime, 
  Expiration_Datetime datetime, 
  Active_Flag tinyint(1) NOT NULL DEFAULT '0',   

  Deleted_Flag tinyint(1) NOT NULL DEFAULT '0', 
  Deleted_Datetime datetime DEFAULT NULL,

  Quiz_ID bigint(20) NOT NULL,
  Account_ID bigint(20) NOT NULL,

  First_Added_Datetime datetime NOT NULL,
  Last_Modified_Datetime datetime NOT NULL,

  Quiz_Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Quiz_Description varchar(2000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Quiz_Type varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  
  Has_Passing_Score_Flag tinyint(1) NOT NULL DEFAULT '0',
  Passing_Score int(11) NOT NULL DEFAULT '0',
  Can_Review_Previous_Slides_Flag tinyint(1) NOT NULL DEFAULT '0' ,
  
  -- Admin Fields
   Type int(11) NOT NULL DEFAULT '0',
   ETL_Run_Datetime datetime,
  
  PRIMARY KEY (ID) 
   
) ENGINE=InnoDB DEFAULT CHARSET=utf8;







 
Drop Table if Exists Status_Dim; 

Create Table Status_Dim
(
  Status_ID  bigint,
  Status_Name varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
--   Status_Description varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  
  Effective_Datetime datetime,
  Expiration_Datetime datetime,
  Active_Flag tinyint(1) default '1'
  
) ;

 




Drop Table if Exists tier_ref; 
Create Table tier_ref
(  ETL_Run_Datetime datetime,
  `ID` int(11) NOT NULL,
  `Name` varchar(500) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Description` varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL  
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 








Drop Table If Exists Trainee_Certificate_Fact ; 

Create Table Trainee_Certificate_Fact

(
TCF_ID bigint not null auto_increment,
TraineeCertificate_ID bigint not null,

Deleted_Flag tinyint(1),
Deleted_Datetime datetime,
Effective_Datetime datetime,
Expiration_Datetime datetime,
Active_Flag int,

-- FKs
Account_ID  bigint,
Trainee_ID bigint,
Course_ID  bigint,
Module_ID bigint,

-- Date / Time fields
First_Added_Date date, 
First_Added_Time time,
First_Added_Datetime datetime,

Last_Modified_Date  Date  , 
Last_Modified_Time time ,
Last_Modified_Datetime  Datetime  ,

Module_Completion_Date date, 
Module_Completion_Time time,
Module_Completion_Datetime datetime,

Quiz_Score int ,

ETL_Run_Datetime datetime,
  
  PRIMARY KEY (TCF_ID) 

) ENGINE=InnoDB DEFAULT CHARSET=utf8;









Drop Table if Exists User_Dim; 

Create Table User_Dim
(
  ID bigint not null auto_increment,
  Effective_Datetime datetime, 
  Expiration_Datetime datetime, 
  Active_Flag tinyint(1) NOT NULL DEFAULT '0',   
  
  Deleted_Flag tinyint(1) NOT NULL DEFAULT '0',   
  Deleted_Datetime datetime, 

  User_ID Bigint(20) NOT NULL,
  Account_ID bigint(20) NOT NULL,
  
  User_Role varchar(20) NOT NULL,    -- (Trainee, Trainer, Administrator,...)
  
  User_Status  char(8) NOT NULL DEFAULT 'Active' ,     --  Status : Active, Archived
  Full_Name varchar(300) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  First_Name varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Last_Name varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  Email varchar(320) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  
  Department varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  TimeZone   varchar(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  First_Added_Datetime datetime not null,   
  Last_Modified_Datetime datetime not null,
   
  Password_Expiration_Flag  tinyint(1) not null default'0',
  Password_Expiration_Datetime datetime DEFAULT NULL,
  
  -- validation check fields.  not visible to user
  TimezoneType Int(11) NOT NULL,
  Status int(11) NOT NULL, 
  Permissions int(11) NOT NULL,    -- User Roles,
  ProductMembership int(11) NOT NULL,
  ETL_Run_Datetime datetime,
  
  PRIMARY KEY (ID) 
  
  
) ;








Drop Table if Exists User_Group_Dim; 

Create Table User_Group_Dim
(
  ID bigint not null auto_increment,
  Effective_Datetime datetime, 
  Expiration_Datetime datetime, 
  Active_Flag tinyint(1) NOT NULL DEFAULT '0',   
  
  Deleted_Flag tinyint(1) NOT NULL DEFAULT '0',   
  Deleted_Date datetime, 

  First_Added_Datetime datetime,
  Last_Modified_Datetime datetime,

  User_Group_ID Bigint(20) NOT NULL,
  User_ID Bigint(20) NOT NULL,
  Account_ID bigint(20) NOT NULL,
  Group_Name varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,

  -- Admin Fields
   ETL_Run_Datetime datetime,
  
  PRIMARY KEY (ID) 
  
) ;







