DECLARE @EndTime DATETIME = GETDATE();
DECLARE @DatabaseName SYSNAME = N'databaseName';
DECLARE @StatsPercentFull TINYINT = 1;
DECLARE @StatsPercentDiff TINYINT = 1;
DECLARE @StatsPercentLog TINYINT = 1;
DECLARE @RestoreFullPropertyDesc NVARCHAR(MAX) = N'STATS = ' + CAST(@StatsPercentFull AS NVARCHAR(6)) + N', NORECOVERY';
DECLARE @RestoreDiffPropertyDesc NVARCHAR(MAX) = N'STATS = ' + CAST(@StatsPercentDiff AS NVARCHAR(6)) + N', REPLACE, NORECOVERY';
DECLARE @RestoreLogPropertyDesc NVARCHAR(MAX) = N'STATS = ' + CAST(@StatsPercentLog AS NVARCHAR(6)) + N', NORECOVERY';

;WITH CTE_backupmediafamily AS (
	SELECT
		backupmediafamily.media_set_id,
		backupmediafamily.family_sequence_number,
		backupmediafamily.physical_device_name,
		backupmediafamily.device_type,
		CASE 
			WHEN backupmediafamily.device_type = 2 THEN N'DISK'
			WHEN backupmediafamily.device_type = 5 THEN N'TAPE'
			WHEN backupmediafamily.device_type = 7 THEN N'VIRTUAL DEVICE'
			WHEN backupmediafamily.device_type = 9 THEN N'Azure Storage'
			WHEN backupmediafamily.device_type = 105 THEN N'A permanent backup device'
				ELSE NULL
		END AS DeviceTypeDesc,
		CASE 
			WHEN backupmediafamily.device_type = 2 THEN N'DISK'
			WHEN backupmediafamily.device_type = 5 THEN N'TAPE'			
			WHEN backupmediafamily.device_type = 7 THEN N'DEVICE'		
			WHEN backupmediafamily.device_type = 9 THEN N'URL'
			WHEN backupmediafamily.device_type = 105 THEN N'DEVICE'
				ELSE NULL
		END AS DeviceTypeUse,	
		(N'''' + backupmediafamily.physical_device_name + N'''') AS physical_device_name_calc		
	FROM msdb.dbo.backupmediafamily
	WHERE
		backupmediafamily.device_type = 2
		AND backupmediafamily.physical_device_name NOT LIKE '{%'
),

CTE_backupList AS (
	SELECT 
		backupset.database_name,
		backupset.backup_start_date,
		backupset.backup_finish_date,
		backupset.media_set_id,
		backupset.type,
		backupset.checkpoint_lsn,
		backupset.database_backup_lsn,
		backupset.differential_base_lsn,
		backupset.first_lsn,
		backupset.last_lsn,
		backupmediafamily.physical_device_name,
		backupmediafamily.physical_device_name_calc,
		backupset.backup_set_id,
		ListRestoreFile.PathFileList	
	FROM msdb.dbo.backupset
		INNER JOIN CTE_backupmediafamily AS backupmediafamily
			ON backupset.media_set_id = backupmediafamily.media_set_id
	
	 CROSS APPLY (SELECT
	      			STUFF((SELECT 
								N', ' + CTE_backupmediafamily.DeviceTypeUse + N' = N' + CTE_backupmediafamily.physical_device_name_calc	     				
	    					FROM CTE_backupmediafamily
	    					WHERE
	     						CTE_backupmediafamily.media_set_id = backupmediafamily.media_set_id
	    					ORDER BY
	     						CTE_backupmediafamily.family_sequence_number
	        				FOR XML PATH('')), 1, 2, '') AS PathFileList) AS ListRestoreFile		
	
	WHERE
		backupset.backup_finish_date <= @EndTime
		AND backupset.database_name = @DatabaseName
		AND backupset.is_copy_only = 0
		AND backupset.is_damaged = 0
		AND backupset.is_snapshot = 0
),

CTE_databaseFileList AS (
	SELECT DISTINCT			
		CTE_backupList.backup_set_id,
		ListMoveFile.MovePathFileList
	FROM CTE_backupList
		CROSS APPLY (SELECT
	      				STUFF((SELECT
	     							N', ' + N'MOVE ' + QUOTENAME(backupfile.logical_name, '''') + N' TO ' + (N'''' + backupfile.physical_name + N'''') 
	    						FROM msdb.dbo.backupfile
	    						WHERE
	     							backupfile.backup_set_id = CTE_backupList.backup_set_id
								GROUP BY
									backupfile.backup_set_id,
									backupfile.logical_name,
									backupfile.physical_name,
									backupfile.file_number
	    						ORDER BY
	     							backupfile.file_number
	        					FOR XML PATH('')), 1, 2, '') AS MovePathFileList) AS ListMoveFile

		INNER JOIN msdb.dbo.backupfile
			ON backupfile.backup_set_id = CTE_backupList.backup_set_id
	WHERE
		CTE_backupList.type = 'D'
)

SELECT
	'Restore FULL + DIFF + Logs' AS DatabaseName,
	@EndTime AS RestoreLimit,
	NULL AS DateBackup,
	NULL AS BackupType,
	NULL AS PathBackup,
	NULL AS LSNBackup,
	NULL AS RestoreCommand

UNION ALL

SELECT 
	BackupList.DatabaseName,
	@EndTime AS RestoreLimit,
	BackupFull.backup_finish_date AS DateBackup,
	BackupFull.type AS BackupType,
	BackupFull.PathFileList AS PathBackup,
	BackupFull.database_backup_lsn AS LSNBackup,
	N'RESTORE DATABASE ' + QUOTENAME(@DatabaseName) + N' FROM ' + BackupFull.PathFileList + N' WITH ' + BackupFull.MovePathFileList + N', ' + @RestoreFullPropertyDesc + N';' AS RestoreCommand
FROM (VALUES (@DatabaseName)) AS BackupList (DatabaseName) 
	CROSS APPLY (SELECT TOP 1					 
					CTE_backupList.backup_start_date,
					CTE_backupList.backup_finish_date,
					CTE_backupList.type,
					CTE_backupList.checkpoint_lsn,
					CTE_backupList.database_backup_lsn,
					CTE_backupList.differential_base_lsn,
					CTE_backupList.first_lsn,
					CTE_backupList.last_lsn,					 
					CTE_backupList.PathFileList,
					CTE_backupList.backup_set_id,
					CTE_databaseFileList.MovePathFileList
				 FROM CTE_backupList
					INNER JOIN CTE_databaseFileList
						ON CTE_databaseFileList.backup_set_id = CTE_backupList.backup_set_id
				 WHERE
					CTE_backupList.database_name = BackupList.DatabaseName
					AND CTE_backupList.backup_finish_date <= @EndTime
					AND CTE_backupList.type = 'D'					
				 ORDER BY
					CTE_backupList.backup_finish_date DESC) AS BackupFull

UNION ALL

SELECT 
	BackupList.DatabaseName,
	@EndTime AS RestoreLimit,
	BackupDiff.backup_finish_date AS DateBackup,
	BackupDiff.type AS BackupType,
	BackupDiff.PathFileList AS PathBackup,
	BackupDiff.database_backup_lsn AS LSNBackup,
	N'RESTORE DATABASE ' + QUOTENAME(@DatabaseName) + N' FROM ' + BackupDiff.PathFileList + N' WITH ' + @RestoreDiffPropertyDesc + N';' AS RestoreCommand
FROM (VALUES (@DatabaseName)) AS BackupList (DatabaseName) 
	CROSS APPLY (SELECT TOP 1					 
					CTE_backupList.backup_start_date,
					CTE_backupList.backup_finish_date,
					CTE_backupList.type,
					CTE_backupList.checkpoint_lsn,
					CTE_backupList.database_backup_lsn,
					CTE_backupList.differential_base_lsn,
					CTE_backupList.first_lsn,
					CTE_backupList.last_lsn,					 
					CTE_backupList.PathFileList,
					CTE_backupList.backup_set_id,
					CTE_databaseFileList.MovePathFileList
				 FROM CTE_backupList
					INNER JOIN CTE_databaseFileList
						ON CTE_databaseFileList.backup_set_id = CTE_backupList.backup_set_id
				 WHERE
					CTE_backupList.database_name = BackupList.DatabaseName
					AND CTE_backupList.backup_finish_date <= @EndTime
					AND CTE_backupList.type = 'D'					
				 ORDER BY
					CTE_backupList.backup_finish_date DESC) AS BackupFull

	OUTER APPLY (SELECT TOP 1					 
					CTE_backupList.backup_start_date,
					CTE_backupList.backup_finish_date,
					CTE_backupList.type,
					CTE_backupList.checkpoint_lsn,
					CTE_backupList.database_backup_lsn,
					CTE_backupList.differential_base_lsn,
					CTE_backupList.first_lsn,
					CTE_backupList.last_lsn,					 
					CTE_backupList.PathFileList
				 FROM CTE_backupList
				 WHERE
					CTE_backupList.database_name = BackupList.DatabaseName					
					AND CTE_backupList.backup_finish_date <= @EndTime
					AND CTE_backupList.type = 'I'
					AND CTE_backupList.differential_base_lsn = CTE_backupList.database_backup_lsn
					AND CTE_backupList.database_backup_lsn = BackupFull.checkpoint_lsn					
				 ORDER BY
					CTE_backupList.backup_finish_date DESC) AS BackupDiff

UNION ALL

SELECT 
	BackupList.DatabaseName,
	@EndTime AS RestoreLimit,
	BackupLog.backup_finish_date AS DateBackup,
	BackupLog.type AS BackupType,
	BackupLog.PathFileList AS PathBackup,
	BackupLog.first_lsn AS LSNBackup,
	N'RESTORE LOG ' + QUOTENAME(@DatabaseName) + N' FROM ' + BackupLog.PathFileList + N' WITH ' + @RestoreLogPropertyDesc + N';' AS RestoreCommand
FROM (VALUES (@DatabaseName)) AS BackupList (DatabaseName) 
	CROSS APPLY (SELECT TOP 1					 
					CTE_backupList.backup_start_date,
					CTE_backupList.backup_finish_date,
					CTE_backupList.type,
					CTE_backupList.checkpoint_lsn,
					CTE_backupList.database_backup_lsn,
					CTE_backupList.differential_base_lsn,
					CTE_backupList.first_lsn,
					CTE_backupList.last_lsn,					 
					CTE_backupList.PathFileList,
					CTE_backupList.backup_set_id,
					CTE_databaseFileList.MovePathFileList
				 FROM CTE_backupList
					INNER JOIN CTE_databaseFileList
						ON CTE_databaseFileList.backup_set_id = CTE_backupList.backup_set_id
				 WHERE
					CTE_backupList.database_name = BackupList.DatabaseName
					AND CTE_backupList.backup_finish_date <= @EndTime
					AND CTE_backupList.type = 'D'					
				 ORDER BY
					CTE_backupList.backup_finish_date DESC) AS BackupFull

	OUTER APPLY (SELECT TOP 1					 
					CTE_backupList.backup_start_date,
					CTE_backupList.backup_finish_date,
					CTE_backupList.type,
					CTE_backupList.checkpoint_lsn,
					CTE_backupList.database_backup_lsn,
					CTE_backupList.differential_base_lsn,
					CTE_backupList.first_lsn,
					CTE_backupList.last_lsn,					 
					CTE_backupList.PathFileList
				 FROM CTE_backupList
				 WHERE
					CTE_backupList.database_name = BackupList.DatabaseName					
					AND CTE_backupList.backup_finish_date <= @EndTime
					AND CTE_backupList.type = 'I'
					AND CTE_backupList.differential_base_lsn = CTE_backupList.database_backup_lsn
					AND CTE_backupList.database_backup_lsn = BackupFull.checkpoint_lsn
					
				 ORDER BY
					CTE_backupList.backup_finish_date DESC) AS BackupDiff
	
	OUTER APPLY (SELECT 					 
					CTE_backupList.backup_start_date,
					CTE_backupList.backup_finish_date,
					CTE_backupList.type,
					CTE_backupList.checkpoint_lsn,
					CTE_backupList.database_backup_lsn,
					CTE_backupList.differential_base_lsn,
					CTE_backupList.first_lsn,
					CTE_backupList.last_lsn,					 
					CTE_backupList.PathFileList
				 FROM CTE_backupList
				 WHERE
					CTE_backupList.database_name = BackupList.DatabaseName					
					AND CTE_backupList.backup_finish_date <= @EndTime
					AND CTE_backupList.type = 'L'					
					AND CTE_backupList.last_lsn > BackupDiff.last_lsn) AS BackupLog

UNION ALL

SELECT
	@DatabaseName AS DatabaseName,
	@EndTime AS RestoreLimit,
	NULL AS DateBackup,
	NULL AS BackupType,
	NULL AS PathBackup,
	NULL AS LSNBackup,
	'RESTORE DATABASE ' + QUOTENAME(@DatabaseName) + ' WITH RECOVERY;' AS RestoreCommand

UNION ALL

SELECT
	'Restore FULL + Logs' AS DatabaseName,
	@EndTime AS RestoreLimit,
	NULL AS DateBackup,
	NULL AS BackupType,
	NULL AS PathBackup,
	NULL AS LSNBackup,
	NULL AS RestoreCommand

UNION ALL

SELECT 
	BackupList.DatabaseName,
	@EndTime AS RestoreLimit,
	BackupFull.backup_finish_date AS DateBackup,
	BackupFull.type AS BackupType,
	BackupFull.PathFileList AS PathBackup,
	BackupFull.database_backup_lsn AS LSNBackup,
	N'RESTORE DATABASE ' + QUOTENAME(@DatabaseName) + N' FROM ' + BackupFull.PathFileList + N' WITH ' + BackupFull.MovePathFileList + N', ' + @RestoreFullPropertyDesc + N';' AS RestoreCommand
FROM (VALUES (@DatabaseName)) AS BackupList (DatabaseName) 
	CROSS APPLY (SELECT TOP 1					 
					CTE_backupList.backup_start_date,
					CTE_backupList.backup_finish_date,
					CTE_backupList.type,
					CTE_backupList.checkpoint_lsn,
					CTE_backupList.database_backup_lsn,
					CTE_backupList.differential_base_lsn,
					CTE_backupList.first_lsn,
					CTE_backupList.last_lsn,					 
					CTE_backupList.PathFileList,
					CTE_backupList.backup_set_id,
					CTE_databaseFileList.MovePathFileList
				 FROM CTE_backupList
					INNER JOIN CTE_databaseFileList
						ON CTE_databaseFileList.backup_set_id = CTE_backupList.backup_set_id
				 WHERE
					CTE_backupList.database_name = BackupList.DatabaseName
					AND CTE_backupList.backup_finish_date <= @EndTime
					AND CTE_backupList.type = 'D'					
				 ORDER BY
					CTE_backupList.backup_finish_date DESC) AS BackupFull

UNION ALL

SELECT 
	BackupList.DatabaseName,
	@EndTime AS RestoreLimit,
	BackupLog.backup_finish_date AS DateBackup,
	BackupLog.type AS BackupType,
	BackupLog.PathFileList AS PathBackup,
	BackupLog.first_lsn AS LSNBackup,
	N'RESTORE LOG ' + QUOTENAME(@DatabaseName) + N' FROM ' + BackupLog.PathFileList + N' WITH ' + @RestoreLogPropertyDesc + N';' AS RestoreCommand
FROM (VALUES (@DatabaseName)) AS BackupList (DatabaseName) 
	CROSS APPLY (SELECT TOP 1					 
					CTE_backupList.backup_start_date,
					CTE_backupList.backup_finish_date,
					CTE_backupList.type,
					CTE_backupList.checkpoint_lsn,
					CTE_backupList.database_backup_lsn,
					CTE_backupList.differential_base_lsn,
					CTE_backupList.first_lsn,
					CTE_backupList.last_lsn,					 
					CTE_backupList.PathFileList,
					CTE_backupList.backup_set_id,
					CTE_databaseFileList.MovePathFileList
				 FROM CTE_backupList
					INNER JOIN CTE_databaseFileList
						ON CTE_databaseFileList.backup_set_id = CTE_backupList.backup_set_id
				 WHERE
					CTE_backupList.database_name = BackupList.DatabaseName
					AND CTE_backupList.backup_finish_date <= @EndTime
					AND CTE_backupList.type = 'D'					
				 ORDER BY
					CTE_backupList.backup_finish_date DESC) AS BackupFull
	
	OUTER APPLY (SELECT 					 
					CTE_backupList.backup_start_date,
					CTE_backupList.backup_finish_date,
					CTE_backupList.type,
					CTE_backupList.checkpoint_lsn,
					CTE_backupList.database_backup_lsn,
					CTE_backupList.differential_base_lsn,
					CTE_backupList.first_lsn,
					CTE_backupList.last_lsn,					 
					CTE_backupList.PathFileList
				 FROM CTE_backupList
				 WHERE
					CTE_backupList.database_name = BackupList.DatabaseName					
					AND CTE_backupList.backup_finish_date <= @EndTime
					AND CTE_backupList.type = 'L'					
					AND CTE_backupList.last_lsn > BackupFull.last_lsn) AS BackupLog

UNION ALL

SELECT
	@DatabaseName AS DatabaseName,
	@EndTime AS RestoreLimit,
	NULL AS DateBackup,
	NULL AS BackupType,
	NULL AS PathBackup,
	NULL AS LSNBackup,
	'RESTORE DATABASE ' + QUOTENAME(@DatabaseName) + ' WITH RECOVERY;' AS RestoreCommand;




--https://luizlima.net/script-restore-restaurando-varios-arquivos-de-backup-de-log-opcoes-stopat-e-standby/

DECLARE @StartDate DATETIME = '2020-08-20 00:00:00.000'

SELECT
	backupset.database_name AS DatabaseName,
	backupmediafamily.physical_device_name AS PathBackup,
	backupset.type,
	CASE
		WHEN backupset.type = 'I' THEN CONCAT('RESTORE DATABASE [', backupset.database_name, '] FROM DISK = ''', backupmediafamily.physical_device_name, ''' WITH NORECOVERY, REPLACE, STATS = 5;')
		WHEN backupset.type = 'L' THEN CONCAT('RESTORE LOG [', backupset.database_name, '] FROM DISK = ''', backupmediafamily.physical_device_name, ''' WITH FILE = 1, NORECOVERY, STATS = 5;')
			ELSE NULL
	END AS RestoreCommand
	,CAST(CAST(backupset.backup_size / 1000000 AS INT) AS VARCHAR(14)) + ' ' + 'MB' AS BackupSize,
	CONCAT(CAST(DATEDIFF(second, backupset.backup_start_date, backupset.backup_finish_date) AS VARCHAR(4)), ' Seconds') AS ElapsedTime,
	backupset.backup_start_date,
	CAST(backupset.first_lsn AS VARCHAR(50)) AS first_lsn,
	CAST(backupset.last_lsn AS VARCHAR(50)) AS last_lsn,
	CASE
		WHEN backupset.[type] = 'D' THEN 'Full'
		WHEN backupset.[type] = 'I' THEN 'Differential'
		WHEN backupset.[type] = 'L' THEN 'Transaction Log'
	END AS BackupType,
	backupset.server_name,
	backupset.recovery_model	
FROM msdb.dbo.backupset
	INNER JOIN msdb.dbo.backupmediafamily
		ON backupset.media_set_id = backupmediafamily.media_set_id
WHERE
	1 = 1
	AND DB_ID(backupset.database_name) > 4
	AND backupset.database_name NOT IN ('dbaMaintenance')
	AND backupset.database_name = DB_NAME() -- Remove this line for all the database
	--AND backupmediafamily.physical_device_name LIKE '%FCIAMB$AMBEVMDM_HBMDM_SAAS_LOG_20200722_062004.trn'
	AND backupset.backup_start_date >= @StartDate
ORDER BY
	backupset.database_name ASC,
	backupset.backup_start_date ASC, 
	backupset.backup_finish_date;



DECLARE @StartDate DATETIME = '2020-07-22 06:20:04.000'
DECLARE @OldPath VARCHAR(400) = '\\10.2.57.89\ambevmdm_log\FCIAMB$AMBEVMDM\HBMDM_SAAS\LOG\'
DECLARE @NewPath VARCHAR(300) = '''G:\New folder\'

SELECT TOP 100
	backupset.database_name AS DatabaseName,
	backupmediafamily.physical_device_name AS PathBackup,
	CONCAT('RESTORE LOG [', DB_NAME(), '] FROM DISK = ', REPLACE(backupmediafamily.physical_device_name, @OldPath, @NewPath), ''' WITH FILE = 1, NORECOVERY;') AS RestorePath,
	CAST(CAST(backupset.backup_size / 1000000 AS INT) AS VARCHAR(14)) + ' ' + 'MB' AS BackupSize,
	CONCAT(CAST(DATEDIFF(second, backupset.backup_start_date, backupset.backup_finish_date) AS VARCHAR(4)), ' Seconds') AS ElapsedTime,
	backupset.backup_start_date,
	CAST(backupset.first_lsn AS VARCHAR(50)) AS first_lsn,
	CAST(backupset.last_lsn AS VARCHAR(50)) AS last_lsn,
	CASE
		WHEN backupset.[type] = 'D' THEN 'Full'
		WHEN backupset.[type] = 'I' THEN 'Differential'
		WHEN backupset.[type] = 'L' THEN 'Transaction Log'
	END AS BackupType,
	backupset.server_name,
	backupset.recovery_model	
FROM msdb.dbo.backupset
	INNER JOIN msdb.dbo.backupmediafamily
		ON backupset.media_set_id = backupmediafamily.media_set_id
WHERE
	backupset.database_name = DB_NAME() -- Remove this line for all the database
	--AND backupmediafamily.physical_device_name LIKE '%FCIAMB$AMBEVMDM_HBMDM_SAAS_LOG_20200722_062004.trn'
	AND backupset.backup_start_date >= @StartDate
ORDER BY
	backupset.backup_start_date ASC, 
	backupset.backup_finish_date;

USE [master]
GO
ALTER DATABASE [HBOCP] SET SINGLE_USER WITH NO_WAIT -- WITH ROLLBACK IMMEDIATE
GO

-- Restore Full
RESTORE DATABASE [HBOCP] FROM DISK = 'H:\Backup\HBOCP\FCIWMS$WMS_HBOCP_FULL_20190616_130010.bak' WITH NORECOVERY, REPLACE, STATS = 5;
	/*,MOVE 'BASE_PRODUCAO_2' TO 'C:\SQLServer\Data\BASE_PRODUCAO_2_TesteRestore_BASE_PRODUCAO.ndf', 
        MOVE 'BASE_PRODUCAO_log' TO 'C:\SQLServer\Log\BASE_PRODUCAO_log_TesteRestore_BASE_PRODUCAO.ldf', 
        MOVE 'BASE_PRODUCAO' TO 'C:\SQLServer\Data\BASE_PRODUCAO_TesteRestore_BASE_PRODUCAO.mdf'  */

GO

--	Restore Diferencial
RESTORE DATABASE [HBOCP] FROM DISK = 'H:\Backup\HBOCP\FCIWMS$WMS_HBOCP_DIFF_20190621_060003.bak' WITH NORECOVERY, REPLACE, STATS = 5;
GO

-- Restore Log
RESTORE LOG [HBOCP] FROM DISK = 'H:\Backup\HBOCP\FCIWMS$WMS_HBOCP_LOG_20190621_070002.trn' WITH FILE = 1, NORECOVERY; -- 2019-03-27 23:12:14
GO
RESTORE LOG [HBOCP] FROM DISK = 'H:\Backup\HBOCP\FCIWMS$WMS_HBOCP_LOG_20190621_080004.trn' WITH FILE = 1, NORECOVERY; -- 2019-03-27 23:13:05
GO
RESTORE LOG [HBOCP] FROM DISK = 'H:\Backup\HBOCP\FCIWMS$WMS_HBOCP_LOG_20190621_090000.trn' WITH FILE = 1, NORECOVERY; -- 2019-03-27 23:14:26
GO
--,STOPAT = '2019-03-27 23:14:00' (para parar em um x momento do tempo, retirar o nonrecovery e inserir este termo)
--,STANDBY = N'C:\SQLServer\Backup\BASE_PRODUCAO_StandBy' (para poder fazer leituras durante os restores. Necess�rio tirar o NORECOVERY para ativar)

RESTORE DATABASE [HBOCP] WITH RECOVERY
GO

--DBCC CHECKDB('TesteRestore_BASE_PRODUCAO')
GO

ALTER DATABASE [HBOCP] SET MULTI_USER WITH NO_WAIT -- WITH ROLLBACK IMMEDIATE
GO




SELECT
	@@SERVERNAME AS Servidor,
	databases.name AS Banco,
	databases.recovery_model_desc AS ModoRecuperacao,		
	backups.backup_finish_date AS DataBackup,
	CASE backupTypeList.[type] 
		WHEN 'D' THEN 'Completo'
		WHEN 'I' THEN 'Incremental'
		WHEN 'L' THEN 'Transacional'
	END AS TipoBackup,
	backups.physical_device_name AS CaminhoBackup,
	backups.backup_start_date,
	backups.backup_finish_date,
	DATEDIFF(second, backups.backup_start_date,	backups.backup_finish_date) AS DurationSeconds,
	DATEDIFF(minute, backups.backup_start_date,	backups.backup_finish_date) AS DurationMin,
	DATEDIFF(hour, backups.backup_start_date,	backups.backup_finish_date) AS DurationH
FROM sys.databases
	CROSS JOIN (SELECT
					backupType.[type]
				FROM (VALUES ('D'), ('I'), ('L')) AS backupType([type])) AS backupTypeList	

	OUTER APPLY (SELECT					
					backupset.[type],
					backupmediafamily.physical_device_name,
					backupset.backup_start_date,
					backupset.backup_finish_date					
				FROM msdb.dbo.backupset
					INNER JOIN (SELECT
									MAX(backupset.backup_set_id) AS Max_backup_set_id,
									backupset.database_name,
									backupset.[type]				
								FROM msdb.dbo.backupset
								GROUP BY
									backupset.database_name,
									backupset.[type]) AS MaxBackup
						ON MaxBackup.Max_backup_set_id = backupset.backup_set_id
					INNER JOIN msdb.dbo.backupmediafamily
						ON backupmediafamily.media_set_id = backupset.media_set_id
				WHERE
					backupset.database_name = databases.name
					AND backupset.[type] = backupTypeList.[type]) AS backups
WHERE	
	databases.name <> 'tempdb'	

ORDER BY
	databases.name
