
use pacifica ; 



-- THIS PROCESS PROVIDES THE INITIAL LOAD OF THE FRAME AND FRAMESTATE HISTORY DATA.


           TRUNCATE TABLE CC_FRAME_LOAD ; 
           
           
			 INSERT INTO CC_Frame_Load
             (
				ETL_Run_Datetime,
				Last_Extract_Datetime,
				ID,
				AccountID,
				Created,
				Modified,
				Deleted,
				Type,
				ContentFileID,
				QuizID,
				QuestionID,
				DisplayIndex,
				CModuleID,
				Hidden,
				ResizeType,
				IsReady,
				IsReplacing,
				VersionNum,
				Product,
				NarrationContentFileID,
				EngagementScore
 				)
                
			 Select Distinct

               NOW() AS  new_extract_datetime, 
  			   NULL AS  previous_extract_datetime,

				ID,
				AccountID,
				Created,
				Modified,
				Deleted,
				Type,
				ContentFileID,
				QuizID,
				QuestionID,
				DisplayIndex,
				CModuleID,
				Hidden,
				ResizeType,
				IsReady,
				IsReplacing,
				VersionNum,
				Product,
				NarrationContentFileID,
				EngagementScore
			
            
			 from Frame
                        
             where   type in ( 108000300, 108000500 )		-- 108000500    Just get Frame Types that identify the last frame Quiz activity. And Quesiton.
 
          --    and modified > previous_extract_datetime    -- comment this out during builk loads
             
           
            ;
        
       --    SELECT * FROM CC_FRAME_LOAD ; 

          TRUNCATE TABLE CC_FRAMESTATE_LOAD ; 
          
          INSERT INTO cc_FrameState_Load

				( `ETL_Run_Datetime`,
				`Last_Extract_Datetime`,
				`ID`,
				`AccountID`,
				`Created`,
				`Modified`,
				`Deleted`,
				`CModuleID`,
				`FrameID`,
				`UserID`,
				`CourseID`,
				`Status`,
				`QuestionPoints`,
			-- 	`QuestionAnswer`,
				`QuestionIsCorrect`,
				`QuestionAttempts`,
				`QuestionListRandomized`,
				`Score`,
				`GradeStatus`,
				`Product`,
				`Duration`,
				`ProgressSeconds`,
				`EngagementScore`,
				`ScoProperties`)

 
				Select 

                NOW() AS  new_extract_datetime, 
  			    NULL AS  previous_extract_datetime,
               
				`ID`,
				`AccountID`,
				`Created`,
				`Modified`,
				`Deleted`,
				`CModuleID`,
				`FrameID`,
				`UserID`,
				`CourseID`,
				`Status`,
				`QuestionPoints`,
			-- 	`QuestionAnswer`,
				`QuestionIsCorrect`,
				`QuestionAttempts`,
				`QuestionListRandomized`,
				`Score`,
				`GradeStatus`,
				`Product`,
				`Duration`,
				`ProgressSeconds`,
				`EngagementScore`,
				`ScoProperties`
          
				from framestate

                 ;
                
 
         --    SELECT * FROM CC_FRAMEstate_LOAD ;        
         
         
          
           CREATE INDEX NDX_F_ID ON CC_FRAME_LOAD (ID) ; 
           
		   CREATE INDEX NDX_F_QUESITONID ON CC_FRAME_LOAD (QUESTIONID) ; 
          
           CREATE INDEX NDX_FS_ID ON CC_FRAMESTATE_LOAD (FRAMEID) ; 
           
           
           
           
			TRUNCATE TABLE CC_FRAMESTATE ; 


			 INSERT INTO CC_Framestate
             (
				ETL_Run_Datetime,
				Last_Extract_Datetime,
				FrameStateID,
				FrameID,
				AccountID,
				QuizID,
				QuestionID,
				Created,
				Modified,
				Deleted,
				UserID,
				CModuleID,
				CourseID,
				FrameType,     
           --      QuestionType,
				Status,
				QuestionPoints,
				QuestionIsCorrect,
				QuestionAttempts,
				QuestionListRandomized,
				Score,
				GradeStatus,
				Product,
				Duration,
				ProgressSeconds,
				EngagementScore,
				ScoProperties,
                AnswerIndexSelected,
                EssayAnswerLength
				)
                
			 Select Distinct

  				new_extract_datetime, 
  				previous_extract_datetime,
		
				fs.ID,
				fs.FrameID,
				fs.AccountID,
				f.QuizID,
				f.QuestionID,
				fs.Created,
				fs.Modified,
				fs.Deleted,
				fs.UserID,
				fs.CModuleID,
				fs.CourseID,
				f.Type,
			-- 	m.name,
				fs.Status,
				fs.QuestionPoints,
				fs.QuestionIsCorrect,
				fs.QuestionAttempts,
				fs.QuestionListRandomized,
				fs.Score,
				fs.GradeStatus,
				fs.Product,
				fs.Duration,
				fs.ProgressSeconds,
				fs.EngagementScore,
				fs.ScoProperties,
				
				Case When substring(fs.scoproperties, instr(fs.scoproperties,"AnswerIndex") + 14 ,  1 ) = "," and q.type = 118000100  Then cast(substring(fs.scoproperties, instr(fs.scoproperties,"AnswerIndex") + 13 ,  1 )  as UNSIGNED)
					 When substring(fs.scoproperties, instr(fs.scoproperties,"AnswerIndex") + 13 ,  1 ) = "-" and q.type = 118000100  Then cast(substring(fs.scoproperties, instr(fs.scoproperties,"AnswerIndex") + 13 ,  2 )   as UNSIGNED)
				End as AnswerIndexSelected ,
				
				ifnull(length(qa.essaytext),0)
            
			 from cc_Framestate_load fs
			 
			 inner join cc_frame_load f
			 on fs.frameid = f.id
             
             
             left outer join question q            -- if it is a question record then get the question type and look it up on mtype
             on f.questionid = q.id
           
             
             left outer join questionanswer qa		-- if it is a question essay record then get the essay answer to calc length
             on qa.framestateid = fs.id
 
            ;
        
          
          
          

-- End of script