/*
--To execute

DECLARE @QtdRowsPerExecution INT = 1000;
DECLARE @DatabaseName NVARCHAR(128);
DECLARE @DatabaseNameCriteria NVARCHAR(128) = '%aster%';
DECLARE @SchemaName SYSNAME = 'dbo';
DECLARE @TableName SYSNAME = 'table';
DECLARE @IdTableColumnName SYSNAME = 'id';
DECLARE @DateColumnTableConditionName SYSNAME = 'date';
DECLARE @AmountDayToMaintain TINYINT = 30;
DECLARE @DisableTriggers BIT = 1;

EXECUTE Procedure_Houskeeping_Table
	@QtdRowsPerExecution = @QtdRowsPerExecution,
	@DatabaseName = @DatabaseName,
	@DatabaseNameCriteria = @DatabaseNameCriteria,
	@SchemaName = @SchemaName,
	@TableName = @TableName,
	@IdTableColumnName = @IdTableColumnName,
	@DateColumnTableConditionName = @DateColumnTableConditionName,
	@AmountDayToMaintain = @AmountDayToMaintain, 
	@DisableTriggers = @DisableTriggers
*/

CREATE OR ALTER PROCEDURE Procedure_Houskeeping_Table (
	@QtdRowsPerExecution INT = 100,
	@DatabaseName NVARCHAR(128) = NULL,
	@DatabaseNameCriteria NVARCHAR(128) = NULL,
	@SchemaName SYSNAME,
	@TableName SYSNAME,
	@IdTableColumnName SYSNAME,
	@DateColumnTableConditionName SYSNAME,
	@AmountDayToMaintain TINYINT,
	@DisableTriggers BIT = 0
)

AS
SET NOCOUNT ON;
BEGIN

	DECLARE @SQLStatement NVARCHAR(MAX);
	DECLARE @IdSelected INT;
	DECLARE @TriggerStatement NVARCHAR(1000);
	DECLARE @sp_executesql NVARCHAR(MAX);

	DECLARE @DatabaseTableList AS TABLE (
		Id INT IDENTITY(1,1) PRIMARY KEY,
		DatabaseName SYSNAME,
		database_id INT,
		SchemaName SYSNAME,
		schema_id INT,
		TableName SYSNAME,
		object_id INT,
		IdTableColumnName SYSNAME,
		DateColumnTableConditionName SYSNAME,	
		AmountDayToMaintain TINYINT,
		QtdRowsPerExecution INT,
		HasTrigger BIT	
	);	
	
	DECLARE @DatabaseList AS TABLE (
		Id INT IDENTITY (1,1) PRIMARY KEY,
		DatabaseName SYSNAME,
		database_id INT	
	);
		
	IF @DisableTriggers IS NULL
		BEGIN
			SET @DisableTriggers = 0;
		END
	
	IF @DatabaseName IS NULL AND @DatabaseNameCriteria IS NULL
		BEGIN
			RAISERROR ('The variables @DatabseName and @DatabaseNameCriteria are null, please fill ONE of them to execute.', 10, 1) WITH NOWAIT;
		END

	IF @AmountDayToMaintain IS NULL
		BEGIN
			RAISERROR ('The variable @AmountDayToMaintain is null, please fill it to execute.', 10, 1) WITH NOWAIT;
		END

	IF @SchemaName IS NULL
		BEGIN
			RAISERROR ('The variable @SchemaName is null, please fill it to execute.', 10, 1) WITH NOWAIT;
		END

	IF @TableName IS NULL
		BEGIN
			RAISERROR ('The variable @TableName is null, please fill it to execute.', 10, 1) WITH NOWAIT;
		END
	
	IF @IdTableColumnName IS NULL
		BEGIN
			RAISERROR ('The variable @IdTableColumnName is null, please fill it to execute.', 10, 1) WITH NOWAIT;
		END

	IF @DateColumnTableConditionName IS NULL
		BEGIN
			RAISERROR ('The variable @DateColumnTableConditionName is null, please fill it to execute.', 10, 1) WITH NOWAIT;
		END

	IF @DatabaseNameCriteria IS NOT NULL
		BEGIN
			INSERT INTO @DatabaseList (DatabaseName, database_id)
			SELECT
				databases.name AS DatabaseName,
				databases.database_id
			FROM sys.databases
			WHERE
				databases.name LIKE @DatabaseNameCriteria;
		END
		
	IF @DatabaseName IS NOT NULL
		BEGIN
			INSERT INTO @DatabaseList (DatabaseName, database_id)
			SELECT
				databases.name AS DatabaseName,
				databases.database_id
			FROM sys.databases
			WHERE
				databases.name = @DatabaseName;		
		END

	WHILE (SELECT COUNT(*) FROM @DatabaseList) > 0
		BEGIN		
			SELECT TOP 1
				@DatabaseName = [@DatabaseList].DatabaseName,
				@IdSelected = [@DatabaseList].Id
			FROM @DatabaseList	

			SET @sp_executesql = QUOTENAME(@DatabaseName) + N'.sys.sp_executesql';
	
			SET @SQLStatement = N'SELECT
										DB_NAME() AS DatabaseName,
										DB_ID() AS database_id,
										schemas.name AS SchemaName,
										schemas.schema_id,
										tables.name AS TableName,
										tables.object_id,
										IdTable.IdTableColumnName,
										DateColumnTableConditionNameTable.DateColumnTableConditionName,
										@AmountDayToMaintainIn AS AmountDayToMaintain,
										@QtdRowsPerExecutionIn AS QtdRowsPerExecution,
										HasTriggerTable.HasTrigger
								FROM sys.tables
									INNER JOIN sys.schemas
										ON schemas.schema_id = tables.schema_id
									CROSS APPLY (SELECT
														columns.name AS IdTableColumnName
													FROM sys.columns
													WHERE
														columns.object_id = tables.object_id
														AND columns.name = @IdTableColumnNameIn) AS IdTable
									CROSS APPLY (SELECT
														columns.name AS DateColumnTableConditionName
													FROM sys.columns
													WHERE
														columns.object_id = tables.object_id
														AND columns.name = @DateColumnTableConditionNameIn) AS DateColumnTableConditionNameTable
	
									OUTER APPLY (SELECT	TOP 1
														1 AS HasTrigger
													FROM sys.triggers
													WHERE
														triggers.parent_id = tables.object_id) AS HasTriggerTable
								WHERE
									tables.name = @TableNameIn
									AND schemas.name = @SchemaNameIn;';		
		
			INSERT INTO @DatabaseTableList (
				DatabaseName,
				database_id,
				SchemaName,
				schema_id,
				TableName,
				object_id,
				IdTableColumnName,
				DateColumnTableConditionName,
				AmountDayToMaintain,
				QtdRowsPerExecution,
				HasTrigger
			)
				
			EXECUTE @sp_executesql 
				@stmt = @SQLStatement,
				@params = N'@TableNameIn AS SYSNAME, @SchemaNameIn AS SYSNAME, @IdTableColumnNameIn AS SYSNAME,	@DateColumnTableConditionNameIn AS SYSNAME, @AmountDayToMaintainIn TINYINT, @QtdRowsPerExecutionIn AS INT',
				@TableNameIn = @TableName,
				@SchemaNameIn = @SchemaName,
				@IdTableColumnNameIn = @IdTableColumnName,
				@DateColumnTableConditionNameIn = @DateColumnTableConditionName,
				@AmountDayToMaintainIn = @AmountDayToMaintain,
				@QtdRowsPerExecutionIn = @QtdRowsPerExecution;	
	
			DELETE FROM @DatabaseList 
			WHERE 
				[@DatabaseList].Id = @IdSelected;
		END
	
		WHILE (SELECT COUNT(*) FROM @DatabaseTableList) > 0
			BEGIN
				SELECT TOP 1
					@IdSelected = [@DatabaseTableList].Id,
					@TriggerStatement = CASE 
											WHEN [@DatabaseTableList].HasTrigger = 1 AND @DisableTriggers = 1 THEN 'ENABLE ALL TRIGGER ON [' + [@DatabaseTableList].SchemaName + N'].[' + [@DatabaseTableList].TableName + N'];'
												ELSE NULL
										END,
					@SQLStatement = CASE 
										WHEN [@DatabaseTableList].Id IS NOT NULL THEN N'DECLARE @RowCount BIGINT; 
																						DECLARE @Data DATETIME = (SELECT FORMAT((GETDATE() - ' + CAST([@DatabaseTableList].AmountDayToMaintain AS VARCHAR(4)) + N'), ''yyyy-MM-dd'')); 
																						DECLARE @TotalRows BIGINT;																							
																						
																						BEGIN TRY '	+ CASE 
																											WHEN [@DatabaseTableList].HasTrigger = 1 AND @DisableTriggers = 1 THEN ' DISABLE ALL TRIGGER ON [' + [@DatabaseTableList].SchemaName + N'].[' + [@DatabaseTableList].TableName + N'];'
																												ELSE ''
																										END + N'SET @TotalRows = (SELECT COUNT(*) FROM [' + [@DatabaseTableList].SchemaName + N'].[' + [@DatabaseTableList].TableName + N'] WHERE [' + [@DatabaseTableList].TableName + N'].[' + [@DatabaseTableList].DateColumnTableConditionName + '] < @Data); ' + CHAR(10) +
																						N' BEGIN TRANSACTION  	
																							WHILE (@TotalRows > 0)  
																								BEGIN	
																									;WITH CTE_Delete AS (
																										SELECT TOP (' + CAST([@DatabaseTableList].QtdRowsPerExecution AS VARCHAR(10)) + ') [' + [@DatabaseTableList].TableName + N'].[' + [@DatabaseTableList].IdTableColumnName + N'] FROM  [' + [@DatabaseTableList].SchemaName + N'].[' + [@DatabaseTableList].TableName + N']
																										WHERE [' + TableName + N'].[' + DateColumnTableConditionName + N'] < @Data ORDER BY [' + TableName + N'].[' + IdTableColumnName + N'] ASC)
																						
																									DELETE [' + [@DatabaseTableList].TableName + N'] FROM [' + [@DatabaseTableList].SchemaName + N'].[' + [@DatabaseTableList].TableName + N'] 
																									INNER JOIN CTE_Delete ON CTE_Delete.[' + [@DatabaseTableList].IdTableColumnName + N'] = [' + [@DatabaseTableList].TableName + N'].[' + [@DatabaseTableList].IdTableColumnName + N'];
																						
																									SET @RowCount = @@ROWCOUNT;
																									SET @TotalRows = @TotalRows - @RowCount;
																								END
																							COMMIT TRANSACTION
																						END TRY
																						BEGIN CATCH
																							IF (XACT_STATE()) = -1
																								BEGIN 
																									ROLLBACK TRANSACTION;  
																								END; 
																							IF (XACT_STATE()) = 1  
																								BEGIN 
																									COMMIT TRANSACTION; 
																								END; 
																						END CATCH'
					ELSE NULL
				END
				FROM @DatabaseTableList
				ORDER BY
					[@DatabaseTableList].Id ASC;			
	
				/*PRINT @SQLStatement;*/

				EXECUTE @sp_executesql 
					@stmt = @SQLStatement;
	
				IF @TriggerStatement IS NOT NULL AND @DisableTriggers = 1
					BEGIN

						/*PRINT 'Enabling triggers';*/

						SET @TriggerStatement = REPLACE(@TriggerStatement, 'DISABLE TRIGGER', 'ENABLE TRIGGER');

						EXECUTE @sp_executesql 
							@stmt = @TriggerStatement;

						/*PRINT 'Enabled triggers';*/
					END			
	
				DELETE FROM @DatabaseTableList WHERE [@DatabaseTableList].Id = @IdSelected;
		END	
END