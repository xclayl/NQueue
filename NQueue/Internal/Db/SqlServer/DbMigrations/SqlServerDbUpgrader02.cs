using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.SqlServer.DbMigrations;

internal class SqlServerDbUpgrader02 : SqlServerAbstractDbUpgrader
{
    public async ValueTask Upgrade(DbTransaction tran)
    {
        var sql = @"
IF COL_LENGTH('NQueue.WorkItem', 'Internal') IS NULL
BEGIN
    ALTER TABLE NQueue.WorkItem ADD [Internal] NVARCHAR(MAX) NULL;
END
GO
IF COL_LENGTH('NQueue.WorkItemCompleted', 'Internal') IS NULL
BEGIN
    ALTER TABLE NQueue.WorkItemCompleted ADD [Internal] NVARCHAR(MAX) NULL;
END


GO



CREATE OR ALTER PROCEDURE [NQueue].[CompleteWorkItem]
	@WorkItemID INT,
	@Now datetimeoffset(7) = NULL
AS
BEGIN


	IF @Now IS NULL
	BEGIN;
		SET @Now = SYSDATETIMEOFFSET();
	END;

    IF @@TRANCOUNT != 0
	BEGIN;
		THROW 51000, 'Please do not run this in a transaction.', 1;   
	END;
	
	BEGIN TRAN;
	BEGIN TRY;
		
		
		DECLARE @LockResult int;  
		EXEC @LockResult = sp_getapplock @Resource = 'NQueue-work-items', @LockMode = 'Exclusive', @LockTimeout=10000;
		IF @LockResult < 0
		BEGIN;
			THROW 51000, 'Lock not granted.', 1;   
		END;


	
		DECLARE @QueueName NVARCHAR(50);

		SELECT @QueueName = QueueName
		FROM [NQueue].WorkItem wi
		WHERE wi.WorkItemId = @WorkItemID;

		

		
		IF @QueueName IS NOT NULL
		BEGIN;			
		
			DECLARE @NextWorkItemID INT;
			DECLARE @NextCreatedAt DATETIMEOFFSET;
			
			SELECT TOP 1 @NextWorkItemID = WorkItemID, @NextCreatedAt = wi.CreatedAt
			FROM [NQueue].WorkItem wi
			WHERE
				wi.QueueName = @QueueName
				AND wi.WorkItemId != @WorkItemID
				AND wi.IsIngested = 1
			ORDER BY wi.WorkItemId
			
	
			IF @NextWorkItemID IS NULL
			BEGIN;
				DELETE q 
				FROM [NQueue].[Queue] q
				WHERE q.[Name] = @QueueName;
			END;
			ELSE
			BEGIN;
				UPDATE q
				SET LockedUntil = @NextCreatedAt,
					NextWorkItemId = @NextWorkItemID,
					ErrorCount = 0
				FROM [NQueue].[Queue] q 
				WHERE q.[Name] = @QueueName
			END;
		END;

		
		
		INSERT INTO [NQueue].[WorkItemCompleted]
				   ([WorkItemId]
				   ,[Url]
				   ,[DebugInfo]
				   ,[CreatedAt]
				   ,[LastAttemptedAt]
				   ,[QueueName]
				   ,[CompletedAt]
				   ,Internal)
		SELECT * FROM (
			 DELETE FROM [NQueue].WorkItem
			 OUTPUT
				deleted.WorkItemId,
				deleted.[Url],
				deleted.[DebugInfo],
				deleted.[CreatedAt],
				deleted.[LastAttemptedAt],
				deleted.[QueueName],
				@Now AS [CompletedAt],
				deleted.[Internal]
			WHERE WorkItemId = @WorkItemID) a


		
		COMMIT TRAN;
      
	END TRY
	BEGIN CATCH    
		DECLARE 
			@ErrorMessage  NVARCHAR(4000), 
			@ErrorSeverity INT, 
			@ErrorState    INT, 
			@ErrorLine    INT;
			
		SELECT 
			@ErrorMessage = ERROR_MESSAGE(), 
			@ErrorSeverity = ERROR_SEVERITY(), 
			@ErrorState = ERROR_STATE(),
			@ErrorLine = ERROR_LINE();

		SET @ErrorMessage = @ErrorMessage + ' line ' + CAST(@ErrorLine AS varchar(20));

		ROLLBACK TRAN;

		-- return the error inside the CATCH block
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState); 
	END CATCH;  
	  
END;	    
GO
CREATE OR ALTER PROCEDURE [NQueue].[EnqueueWorkItem]
	@Url nvarchar(1000),
	@QueueName nvarchar(50) = NULL,
	@DebugInfo nvarchar(1000) = NULL,
	@Now datetimeoffset(7) = NULL,
	@DuplicateProtection BIT = NULL,
	@Internal NVARCHAR(MAX) = NULL
AS
BEGIN
	IF @Now IS NULL
	BEGIN;
		SET @Now = SYSDATETIMEOFFSET();
	END;

	-- DECLARE @Now datetimeoffset(7) = sysdatetimeoffset() AT TIME ZONE 'GMT Standard Time';
	-- select * from sys.time_zone_info
	
	IF @QueueName IS NULL
	BEGIN;
		SET @QueueName = CONVERT(varchar(255),NEWID());
	END;

	IF @DuplicateProtection IS NULL
	BEGIN;
		SET @DuplicateProtection = 0;
	END;

	IF @DuplicateProtection = 1
	BEGIN;
		DECLARE @DupeWorkItemID INT;

		SELECT TOP 1 @DupeWorkItemID = wi.WorkItemId
		FROM [NQueue].WorkItem wi 
		WHERE
				wi.LastAttemptedAt IS NULL
				AND wi.QueueName = @QueueName
				AND wi.[Url] = @Url;

		IF @DupeWorkItemID IS NOT NULL
		BEGIN;
			DECLARE @DupeWorkItemID2 INT;
			DECLARE @DupeLastAttemptedAt DATETIMEOFFSET;

			SELECT @DupeWorkItemID2 = wi.WorkItemID, @DupeLastAttemptedAt = LastAttemptedAt
			FROM [NQueue].WorkItem wi WITH (REPEATABLEREAD) -- [NQueue].[NextWorkItem] won't be allowed to take this record until the transaction completes
			WHERE wi.WorkItemId = @DupeWorkItemID

			IF @DupeWorkItemID2 IS NOT NULL AND @DupeLastAttemptedAt IS NULL
			BEGIN;			
				RETURN;
			END

		END;

	END;


	INSERT INTO [NQueue].WorkItem ([Url], DebugInfo, CreatedAt, QueueName, IsIngested, Internal)
	VALUES (@Url, @DebugInfo, @Now, @QueueName, 0, @Internal)

END	    
GO
CREATE OR ALTER PROCEDURE [NQueue].[FailWorkItem]
	@WorkItemID INT,
	@Now datetimeoffset(7) = NULL
AS
BEGIN

	IF @Now IS NULL
	BEGIN;
		SET @Now = SYSDATETIMEOFFSET();
	END;
	
	UPDATE q
	SET ErrorCount = q.ErrorCount + 1, LockedUntil = DATEADD(minute, 5, @Now)
	FROM [NQueue].WorkItem r
	JOiN [NQueue].[Queue] q ON r.QueueName = q.[Name]
	WHERE r.WorkItemId = @WorkItemID;

END	    
GO
CREATE OR ALTER PROCEDURE [NQueue].[NextWorkItem]
	@Now datetimeoffset(7) = NULL
AS
BEGIN

	IF @Now IS NULL
	BEGIN;
		SET @Now = SYSDATETIMEOFFSET();
	END;

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    IF @@TRANCOUNT != 0
	BEGIN;
		THROW 51000, 'Please do not run this in a transaction.', 1;   
	END;


	BEGIN TRAN;
	BEGIN TRY
		
		DECLARE @LockResult int;  
		EXEC @LockResult = sp_getapplock @Resource = 'NQueue-work-items', @LockMode = 'Exclusive', @LockTimeout=10000;
		IF @LockResult < 0
		BEGIN;
			THROW 51000, 'Lock not granted.', 1;   
		END;

		-- import work items

		WITH cte AS (			
			SELECT 
				wi.WorkItemId, 
				wi.QueueName, 
				wi.CreatedAt, 
				ROW_NUMBER() OVER (Partition By wi.QueueName ORDER BY wi.WorkItemId) AS RN
			FROM [NQueue].WorkItem wi
			WHERE
				wi.IsIngested = 0
		)
		INSERT INTO [NQueue].[Queue] ([Name], NextWorkItemId, ErrorCount, LockedUntil)
		SELECT cte.QueueName, cte.WorkItemId, 0, cte.CreatedAt
		FROM cte
		WHERE RN = 1
		AND cte.QueueName NOT IN (
			SELECT q.[Name] FROM [NQueue].[Queue] q
		);
		
		
		UPDATE wi
		SET IsIngested = 1
		FROM	
			[NQueue].WorkItem wi
		JOIN
			[NQueue].[Queue] q ON wi.QueueName = q.[Name]
		WHERE
			wi.IsIngested = 0;



		-- take work item

		DECLARE @QueueID INT;
		DECLARE @WorkItemID INT;

		SELECT TOP 1
			@QueueID = q.QueueId,
			@WorkItemID = q.NextWorkItemId
		FROM
			[NQueue].[Queue] q
		WHERE
			q.LockedUntil < @Now
			AND q.ErrorCount < 5
		ORDER BY
			q.LockedUntil, q.NextWorkItemId;
			


		IF @WorkItemID IS NOT NULL
		BEGIN;			
			UPDATE ur 
			SET LastAttemptedAt = @Now
			FROM [NQueue].WorkItem ur
			WHERE ur.WorkItemId = @WorkItemID;
				
			UPDATE ur 
			SET LockedUntil = DATEADD(hour, 1, @Now)
			FROM [NQueue].[Queue] ur
			WHERE ur.QueueId = @QueueID;
		END;

		SELECT r.WorkItemId, r.[Url], r.[Internal]
			FROM [NQueue].WorkItem r	
			WHERE r.WorkItemId = @WorkItemID;

		COMMIT TRAN;
	END TRY 
	BEGIN CATCH    
		DECLARE 
			@ErrorMessage  NVARCHAR(4000), 
			@ErrorSeverity INT, 
			@ErrorState    INT, 
			@ErrorLine    INT;
			
		SELECT 
			@ErrorMessage = ERROR_MESSAGE(), 
			@ErrorSeverity = ERROR_SEVERITY(), 
			@ErrorState = ERROR_STATE(),
			@ErrorLine = ERROR_LINE();

		SET @ErrorMessage = @ErrorMessage + ' line ' + CAST(@ErrorLine AS varchar(20));

		ROLLBACK TRAN;

		-- return the error inside the CATCH block
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState); 
	END CATCH;  
END    
GO
CREATE OR ALTER PROCEDURE [NQueue].[PurgeWorkItems]
	@Now datetimeoffset(7) = NULL
AS
BEGIN	

	IF @Now IS NULL
	BEGIN;
		SET @Now = SYSDATETIMEOFFSET();
	END;

	DELETE TOP (1000) c 
	FROM [NQueue].WorkItemCompleted c
	WHERE c.CompletedAt < DATEADD(day, -14, @Now);
END			
GO
CREATE OR ALTER PROCEDURE [NQueue].[ReplayWorkItem]
	@WorkItemID INT,
	@Now datetimeoffset(7) = NULL
AS
BEGIN

	IF @Now IS NULL
	BEGIN;
		SET @Now = SYSDATETIMEOFFSET();
	END;

	INSERT INTO [NQueue].[WorkItem]
           ([Url]
			,[DebugInfo]
			,[CreatedAt]
			,[QueueName]
			,IsIngested
			,Internal)
     SELECT
		   c.Url,
		   c.DebugInfo,
		   @Now,
		   c.QueueName,
		   0,
		   NULL
	FROM [NQueue].WorkItemCompleted c 
	WHERE c.WorkItemId = @WorkItemID;

END
        ";
        
        
        var batches = SplitIntoBatches(sql);

        foreach (var batch in batches)
        {
            await AbstractWorkItemDb.ExecuteNonQuery(tran, batch);
        }
    }
    
    
    
    
}