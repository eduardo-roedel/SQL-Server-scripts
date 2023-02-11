--Single database
DECLARE @Size INT = 61444;
DECLARE @MaxSize INT = 71444;
DECLARE @CollectDate AS DATETIME = GETDATE();

;WITH CTE_DatabaseFiles AS (
	SELECT TOP 100 PERCENT
		databases.database_id AS DatabaseId,
		DB_NAME() AS DatabaseName,
		ISNULL(filegroups.name, 'logfile') AS FileGroupName,
		database_files.name AS FileLogicalName,
		database_files.file_id AS FileId,
		databases.recovery_model_desc AS RecoveryModel,
		databases.log_reuse_wait_desc,
		(CAST(database_files.type AS VARCHAR) + ' - ' + database_files.type_desc) AS FileType,	
		database_files.physical_name AS [PhysicalFileName],
		CAST(ROUND((database_files.size / 128.0), 2) AS DECIMAL (15, 2)) AS FileSizeMB,	
		CAST(ROUND((FILEPROPERTY(database_files.name, 'spaceused') / 128.0), 2) AS DECIMAL (15, 2)) AS FileUsedSpaceMB,		
		CAST(ROUND((CASE
						WHEN database_files.max_size = -1 THEN 0
							ELSE database_files.max_size / 128
					END), 2) AS DECIMAL (15, 2)) AS FileMaxSizeMB,
		CAST(ROUND((CASE
						WHEN database_files.is_percent_growth = 1 THEN database_files.growth
							ELSE database_files.growth / 128.0
					END), 2) AS DECIMAL(8, 2)) AS FileAutoGrowth,
		CASE
			WHEN database_files.is_percent_growth = 1 THEN '%'
				ELSE 'MB'
		END AS FileUnityAutoGrowth,	
		CAST(((dm_os_volume_stats.total_bytes / 1024.0) / 1024.0) AS DECIMAL (15, 2)) AS DiskSizeMB,
		CAST(((dm_os_volume_stats.available_bytes / 1024.0) / 1024.0) AS DECIMAL (15, 2)) AS DiskFreeSpaceMB,
		@CollectDate AS CollectDate,
		'ALTER DATABASE ' + QUOTENAME(databases.name) + ' MODIFY FILE (NAME = N' + QUOTENAME(database_files.name, '''') + ' , SIZE = ' +	CAST(@Size AS VARCHAR) + 'MB, MAXSIZE = ' + CAST(@MaxSize AS VARCHAR) + 'MB);' COLLATE DATABASE_DEFAULT AS AlterDatabase	
	FROM sys.database_files
		CROSS APPLY sys.dm_os_volume_stats (DB_ID(DB_NAME()), database_files.file_id)
		INNER JOIN sys.databases
			ON databases.database_id = dm_os_volume_stats.database_id	
		LEFT JOIN sys.filegroups
			ON filegroups.data_space_id = database_files.data_space_id
)

SELECT
	CTE_DatabaseFiles.CollectDate,
	CTE_DatabaseFiles.DatabaseId,
	CTE_DatabaseFiles.DatabaseName,
	CTE_DatabaseFiles.FileGroupName,
	CTE_DatabaseFiles.FileLogicalName,
	CTE_DatabaseFiles.FileId,
	CTE_DatabaseFiles.RecoveryModel,
	CTE_DatabaseFiles.FileType,
	CTE_DatabaseFiles.PhysicalFileName,
	CTE_DatabaseFiles.FileSizeMB,
	CTE_DatabaseFiles.FileMaxSizeMB,
	CTE_DatabaseFiles.FileUsedSpaceMB,
	(CTE_DatabaseFiles.FileSizeMB - CTE_DatabaseFiles.FileUsedSpaceMB) AS FileFreeSpaceMB,
	CAST(ROUND(((CTE_DatabaseFiles.FileUsedSpaceMB * 100) / CTE_DatabaseFiles.FileSizeMB), 2) AS DECIMAL (5, 2)) AS FilePercentUsed,	
	CTE_DatabaseFiles.FileAutoGrowth,
	CTE_DatabaseFiles.FileUnityAutoGrowth,
	CTE_DatabaseFiles.DiskSizeMB,
	(CTE_DatabaseFiles.DiskSizeMB - CTE_DatabaseFiles.DiskFreeSpaceMB) AS DiskUsedSpaceMB,
	CTE_DatabaseFiles.DiskFreeSpaceMB,
	CAST(ROUND((((CTE_DatabaseFiles.DiskSizeMB - CTE_DatabaseFiles.DiskFreeSpaceMB) * 100) / CTE_DatabaseFiles.DiskSizeMB), 2) AS DECIMAL(5,2)) AS DiskPercentUsed,
	CTE_DatabaseFiles.AlterDatabase
FROM CTE_DatabaseFiles;

GO

/*

--Multiple databases
SET NOCOUNT ON;

DECLARE @Size INT = 61444;
DECLARE @MaxSize INT = 71444;
DECLARE @CollectDate AS DATETIME = GETDATE();
DECLARE @DatabaseName AS SYSNAME;
DECLARE @sp_executesql NVARCHAR(MAX);
DECLARE @Command NVARCHAR(MAX);
DECLARE @DatabaseCount SMALLINT;

DECLARE @DatabaseList AS TABLE (
	DatabaseName SYSNAME PRIMARY KEY
);

IF OBJECT_ID('tempdb..#DatabaseFile') IS NOT NULL
	BEGIN
		DROP TABLE #DatabaseFile;
	END

CREATE TABLE #DatabaseFile (
	CollectDate DATETIME,
	DatabaseId SMALLINT,
	DatabaseName SYSNAME,
	FIleGroupName SYSNAME,
	FileLogicalName SYSNAME,
	FileId INT,
	RecoveryModel NVARCHAR(120),
	FileType VARCHAR(20),
	PhysicalFileName NVARCHAR(520),
	FileSizeMB DECIMAL(15, 2),
	FileMaxSizeMB DECIMAL(15, 2),
	FileUsedSpaceMB DECIMAL(15, 2),
	FileFreeSpaceMB DECIMAL(15, 2),
	FilePercentUsed DECIMAL(5, 2),
	FileAutoGrowth DECIMAL(8, 2),
	FileUnityAutoGrowth VARCHAR(2),
	DiskSizeMB DECIMAL (15, 2),
	DiskUsedSpaceMB DECIMAL (15, 2),
	DiskFreeSpaceMB DECIMAL (15, 2),
	DiskPercentUsed DECIMAL (5, 2),
	AlterDatabase NVARCHAR(MAX)
);

INSERT INTO @DatabaseList (DatabaseName)
SELECT
	databases.name
FROM sys.databases;

SET @DatabaseCount = @@ROWCOUNT;

SET @Command = N';WITH CTE_DatabaseFiles AS (
					SELECT TOP 100 PERCENT
						databases.database_id AS DatabaseId,
						DB_NAME() AS DatabaseName,
						ISNULL(filegroups.name, ''logfile'') AS FileGroupName,
						database_files.name AS FileLogicalName,
						database_files.file_id AS FileId,
						databases.recovery_model_desc AS RecoveryModel,
						(CAST(database_files.type AS VARCHAR) + '' - '' + database_files.type_desc) AS FileType,	
						database_files.physical_name AS PhysicalFileName,
						CAST(ROUND((database_files.size / 128.0), 2) AS DECIMAL (15, 2)) AS FileSizeMB,	
						CAST(ROUND((FILEPROPERTY(database_files.name, ''spaceused'') / 128.0), 2) AS DECIMAL (15, 2)) AS FileUsedSpaceMB,		
						CAST(ROUND((CASE
										WHEN database_files.max_size = -1 THEN 0
											ELSE database_files.max_size / 128
						END), 2) AS DECIMAL (15, 2)) AS FileMaxSizeMB,
						CAST(ROUND((CASE
										WHEN database_files.is_percent_growth = 1 THEN database_files.growth
											ELSE database_files.growth / 128.0
									END), 2) AS DECIMAL(8, 2)) AS FileAutoGrowth,
						CASE
							WHEN database_files.is_percent_growth = 1 THEN ''%''
								ELSE ''MB''
						END AS FileUnityAutoGrowth,	
						CAST(((dm_os_volume_stats.total_bytes / 1024.0) / 1024.0) AS DECIMAL (15, 2)) AS DiskSizeMB,
						CAST(((dm_os_volume_stats.available_bytes / 1024.0) / 1024.0) AS DECIMAL (15, 2)) AS DiskFreeSpaceMB,
						@CollectDate_In AS CollectDate,
						''ALTER DATABASE '' + QUOTENAME(databases.name) + '' MODIFY FILE (NAME = N'' + QUOTENAME(database_files.name, '''''''') + '' , SIZE = '' + CAST(@Size_In AS VARCHAR) + ''MB, MAXSIZE = '' + CAST(@MaxSize_In AS VARCHAR) + ''MB);'' COLLATE DATABASE_DEFAULT AS AlterDatabase	
					FROM sys.database_files
						CROSS APPLY sys.dm_os_volume_stats (DB_ID(DB_NAME()), database_files.file_id)
						INNER JOIN sys.databases
							ON databases.database_id = dm_os_volume_stats.database_id	
						LEFT JOIN sys.filegroups
							ON filegroups.data_space_id = database_files.data_space_id
				)

				SELECT
					CTE_DatabaseFiles.CollectDate,
					CTE_DatabaseFiles.DatabaseId,
					CTE_DatabaseFiles.DatabaseName,
					CTE_DatabaseFiles.FileGroupName,
					CTE_DatabaseFiles.FileLogicalName,
					CTE_DatabaseFiles.FileId,
					CTE_DatabaseFiles.RecoveryModel,
					CTE_DatabaseFiles.FileType,
					CTE_DatabaseFiles.PhysicalFileName,
					CTE_DatabaseFiles.FileSizeMB,
					CTE_DatabaseFiles.FileMaxSizeMB,
					CTE_DatabaseFiles.FileUsedSpaceMB,
					(CTE_DatabaseFiles.FileSizeMB - CTE_DatabaseFiles.FileUsedSpaceMB) AS FileFreeSpaceMB,
					CAST(ROUND(((CTE_DatabaseFiles.FileUsedSpaceMB * 100) / CTE_DatabaseFiles.FileSizeMB), 2) AS DECIMAL (5, 2)) AS FilePercentUsed,	
					CTE_DatabaseFiles.FileAutoGrowth,
					CTE_DatabaseFiles.FileUnityAutoGrowth,
					CTE_DatabaseFiles.DiskSizeMB,
					(CTE_DatabaseFiles.DiskSizeMB - CTE_DatabaseFiles.DiskFreeSpaceMB) AS DiskUsedSpaceMB,
					CTE_DatabaseFiles.DiskFreeSpaceMB,
					CAST(ROUND((((CTE_DatabaseFiles.DiskSizeMB - CTE_DatabaseFiles.DiskFreeSpaceMB) * 100) / CTE_DatabaseFiles.DiskSizeMB), 2) AS DECIMAL(5,2)) AS DiskPercentUsed,
					CTE_DatabaseFiles.AlterDatabase COLLATE DATABASE_DEFAULT
				FROM CTE_DatabaseFiles;';

WHILE @DatabaseCount > 0
	BEGIN
		SET @DatabaseName = (SELECT TOP 1 [@DatabaseList].DatabaseName FROM @DatabaseList);

		SET @sp_executesql = QUOTENAME(@DatabaseName) + N'.sys.sp_executesql';				

		INSERT INTO #DatabaseFile (
			CollectDate, 
			DatabaseId,
			DatabaseName,
			FIleGroupName,
			FileLogicalName,
			FileId,
			RecoveryModel,
			FileType,
			PhysicalFileName,
			FileSizeMB,
			FileMaxSizeMB,
			FileUsedSpaceMB,
			FileFreeSpaceMB,
			FilePercentUsed,
			FileAutoGrowth,
			FileUnityAutoGrowth,
			DiskSizeMB,
			DiskUsedSpaceMB,
			DiskFreeSpaceMB,
			DiskPercentUsed,
			AlterDatabase
		)

		EXECUTE @sp_executesql 
			@stmt = @Command,
			@params = N'@CollectDate_In AS DATETIME, @Size_In AS INT, @MaxSize_In AS INT',
			@CollectDate_In = @CollectDate,
			@Size_In = @Size,
			@MaxSize_In = @MaxSize;

		SET @DatabaseCount = @DatabaseCount - 1;

		DELETE FROM @DatabaseList WHERE [@DatabaseList].DatabaseName = @DatabaseName;		
	
	END

SET NOCOUNT OFF;

SELECT 
	CollectDate,
	DatabaseId,
	DatabaseName,
	FIleGroupName,
	FileLogicalName,
	FileId,
	--RecoveryModel,
	FileType,
	--PhysicalFileName,
	REPLACE(FileSizeMB, '.', ',') AS FileSizeMB,
	REPLACE(FileMaxSizeMB, '.', ',') AS FileMaxSizeMB,
	REPLACE(FileUsedSpaceMB, '.', ',') AS FileUsedSpaceMB,
	REPLACE(FileFreeSpaceMB, '.', ',') AS FileFreeSpaceMB,
	REPLACE(FilePercentUsed, '.', ',') AS FilePercentUsed,
	REPLACE(FileAutoGrowth, '.', ',') AS FileAutoGrowth,
	REPLACE(FileUnityAutoGrowth, '.', ',') AS FileUnityAutoGrowth,
	--DiskSizeMB,
	--DiskUsedSpaceMB,
	--DiskFreeSpaceMB,
	--DiskPercentUsed,
	AlterDatabase
FROM #DatabaseFile
*/