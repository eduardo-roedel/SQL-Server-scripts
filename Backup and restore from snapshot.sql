USE [database];
GO
CREATE OR ALTER PROCEDURE stpExecuteBackupCreateSnapshot (
	@DatabaseName SYSNAME,
	@PathBackupDiff NVARCHAR(255),
	@PathSnapshot NVARCHAR(3000)
)

AS
SET NOCOUNT ON;
	BEGIN
		DECLARE @BackupCommand NVARCHAR(MAX);
		DECLARE @SnapshotCommand NVARCHAR(MAX);
		DECLARE @DatabaseNameSnapshot SYSNAME = @DatabaseName + N'Atualizacao';
		DECLARE @Depth TINYINT = 1;	
		DECLARE @FilePath NVARCHAR(255);
		DECLARE @DirTree AS TABLE (
			Id BIGINT IDENTITY(1,1) PRIMARY KEY,
			SubDirectory NVARCHAR(255),
			Depth TINYINT
		);	
		
		INSERT INTO @DirTree (SubDirectory, Depth)
		EXECUTE master.sys.xp_dirtree @PathBackupDiff, @Depth, 0;		

		IF NOT EXISTS (SELECT UPPER([@DirTree].SubDirectory) FROM @DirTree WHERE [@DirTree].SubDirectory = 'DIFF')
			BEGIN
				SET @FilePath = @PathBackupDiff + N'\Diff';
				EXECUTE master.dbo.xp_create_subdir @FilePath
			END
		
		--Execute backup DIFF;	
		SET @FilePath += N'\' + @DatabaseName + N'_DIFF_Atualizacao_' + FORMAT(GETDATE(), 'yyyyMMdd_HHmmss') + N'.bak'; 
		
		SET @BackupCommand = N'BACKUP DATABASE ' + QUOTENAME(@DatabaseName) + ' TO DISK = N' + QUOTENAME(@FilePath, '''') + N' WITH DIFFERENTIAL, NOFORMAT, NOSKIP , STATS = 1, CHECKSUM, COMPRESSION;';

		EXECUTE sp_executesql @stmt = @BackupCommand;			

		--Drop Snapshot
		IF EXISTS (SELECT
						databases.name
				   FROM sys.databases
				   WHERE
						databases.name = @DatabaseNameSnapshot)
			BEGIN
				SET @SnapshotCommand = N'DROP DATABASE ' + QUOTENAME(@DatabaseNameSnapshot) + N';';
				PRINT @SnapshotCommand;
				EXECUTE sp_executesql @stmt = @SnapshotCommand;
			END						

		--Recreate Snapshot
		SET @SnapshotCommand = (SELECT
									STUFF((SELECT 			
												N',(NAME = ' + QUOTENAME(FileList.name) + N', FILENAME = N' + FileList.physical_name + N')'
											FROM (SELECT 			
														master_files.name,				
														QUOTENAME(REPLACE(REPLACE((@PathSnapshot + RIGHT(master_files.physical_name, CHARINDEX('\', REVERSE(master_files.physical_name)))), N'.mdf', N'.ss'), N'.ndf', N'.ss'),'''') AS physical_name				
													FROM sys.master_files
													WHERE
														master_files.database_id = DB_ID(@DatabaseName)
														AND master_files.type = 0) AS FileList FOR XML PATH('')), 1, 1, '') AS FileDatabaseList);
		
		
		SET @SnapshotCommand = N'CREATE DATABASE ' + QUOTENAME(@DatabaseNameSnapshot) + N' ON ' + @SnapshotCommand + N' AS SNAPSHOT OF ' + QUOTENAME(@DatabaseName) + N';';
		
		PRINT @SnapshotCommand;

		EXECUTE sp_executesql @stmt = @SnapshotCommand;
		
	END
GO

USE [database];
GO

CREATE PROCEDURE stpExecuteRestoreDatabaseFromSnapshot (
	@DatabaseName SYSNAME,
	@DatabaseNameSnapshot SYSNAME,
	@isToDropSnapshotAfterRestore BIT
)
AS
SET NOCOUNT ON;
	BEGIN
		DECLARE @SnapshotCommand NVARCHAR(4000);		

		IF @isToDropSnapshotAfterRestore IS NULL
			BEGIN
				RAISERROR (N'The @isToDropSnapshotAfterRestore parameter value is not permitted.', 16, 1) WITH NOWAIT;
			END
			
		SET @SnapshotCommand = N'USE [master]; ALTER DATABASE ' + QUOTENAME(@DatabaseName) + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE;';

		PRINT @SnapshotCommand;

		EXECUTE sp_executesql @stmt = @SnapshotCommand;

		SET @SnapshotCommand = N'RESTORE DATABASE ' + QUOTENAME(@DatabaseName) + N' FROM DATABASE_SNAPSHOT = ' + QUOTENAME(@DatabaseNameSnapshot, '''') + ';';

		PRINT @SnapshotCommand;

		EXECUTE sp_executesql @stmt = @SnapshotCommand;

		SET @SnapshotCommand = N'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + N' SET MULTI_USER;';
								 
		PRINT @SnapshotCommand;

		EXECUTE sp_executesql @stmt = @SnapshotCommand;

		IF @isToDropSnapshotAfterRestore = 1
			BEGIN
				SET @SnapshotCommand = N'DROP DATABASE ' + QUOTENAME(@DatabaseNameSnapshot) + ';';
				
				EXECUTE sp_executesql @stmt = @SnapshotCommand;
			END

	END

--With Ola

USE [database];
GO
CREATE PROCEDURE stpExecuteBackupCreateSnapshot (
	@DatabaseName SYSNAME,
	@PathBackupDiff NVARCHAR(3000),
	@PathSnapshot NVARCHAR(3000)
)

AS
SET NOCOUNT ON;
	BEGIN
				
		DECLARE @SnapshotCommand NVARCHAR(MAX);
		DECLARE @DatabaseNameSnapshot SYSNAME = @DatabaseName + N'Atualizacao';

		--Execute backup DIFF;
		EXECUTE [dbo].[DatabaseBackup]
		@Databases = @DatabaseName,
		@Directory = @PathBackupDiff,
		@BackupType = 'DIFF',
		@Verify = 'Y',
		@Compress = 'Y',
		@DatabasesInParallel = 'Y',
		@CleanupMode = 'AFTER_BACKUP',
		@FileName = '{ServerName}${InstanceName}_{DatabaseName}_{BackupType}_Atualizacao_{Year}{Month}{Day}_{Hour}{Minute}{Second}.{FileExtension}',
		@CleanupTime = 168,
		@CheckSum = 'Y',
		@LogToTable = 'Y',
		@Execute = 'Y';		

		--Drop Snapshot
		IF EXISTS (SELECT
						databases.name
				   FROM sys.databases
				   WHERE
						databases.name = @DatabaseNameSnapshot)
			BEGIN
				SET @SnapshotCommand = N'DROP DATABASE ' + QUOTENAME(@DatabaseNameSnapshot) + N';';
				PRINT @SnapshotCommand;
				EXECUTE sp_executesql @stmt = @SnapshotCommand;
			END						

		--Recreate Snapshot
		SET @SnapshotCommand = (SELECT
									STUFF((SELECT 			
												N',(NAME = ' + QUOTENAME(FileList.name) + N', FILENAME = N' + FileList.physical_name + N')'
											FROM (SELECT 			
														master_files.name,				
														QUOTENAME(REPLACE(REPLACE((@PathSnapshot + RIGHT(master_files.physical_name, CHARINDEX('\', REVERSE(master_files.physical_name)))), N'.mdf', N'.ss'), N'.ndf', N'.ss'),'''') AS physical_name				
													FROM sys.master_files
													WHERE
														master_files.database_id = DB_ID(@DatabaseName)
														AND master_files.type = 0) AS FileList FOR XML PATH('')), 1, 1, '') AS FileDatabaseList);
		
		
		SET @SnapshotCommand = N'CREATE DATABASE ' + QUOTENAME(@DatabaseNameSnapshot) + N' ON ' + @SnapshotCommand + N' AS SNAPSHOT OF ' + QUOTENAME(@DatabaseName) + N';';
		
		PRINT @SnapshotCommand;

		EXECUTE sp_executesql @stmt = @SnapshotCommand;
		
	END
GO

USE [database];
GO

CREATE PROCEDURE stpExecuteRestoreDatabaseFromSnapshot (
	@DatabaseName SYSNAME,
	@DatabaseNameSnapshot SYSNAME
)
AS
SET NOCOUNT ON;
	BEGIN
		DECLARE @SnapshotCommand NVARCHAR(4000);

		--Caso houver mais de um snapshot para a mesma base, é necessário remover.
		IF EXISTS (SELECT
						databases.name
					FROM sys.databases
					WHERE
						databases.name = N'<snapshotName>')
			BEGIN
				SET @SnapshotCommand = N'DROP DATABASE [<snapshotName>]';
				PRINT @SnapshotCommand;
				EXECUTE sp_executesql @stmt = @SnapshotCommand;
			END				
			
		SET @SnapshotCommand = N'USE [master]; ALTER DATABASE ' + QUOTENAME(@DatabaseName) + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE;';

		PRINT @SnapshotCommand;

		EXECUTE sp_executesql @stmt = @SnapshotCommand;

		SET @SnapshotCommand = N'RESTORE DATABASE ' + QUOTENAME(@DatabaseName) + N' FROM DATABASE_SNAPSHOT = ' + QUOTENAME(@DatabaseNameSnapshot, '''') + ';';

		PRINT @SnapshotCommand;

		EXECUTE sp_executesql @stmt = @SnapshotCommand;

		SET @SnapshotCommand = N'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + N' SET MULTI_USER;';
								 
		PRINT @SnapshotCommand;

	END