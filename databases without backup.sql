USE Traces;
GO

--DECLARE @backupStart DATETIME = '2022-07-16 18:00:00.000';
--DECLARE @backupFinish DATETIME = '2022-07-18 18:00:00.000';
--DECLARE @backupType CHAR(1) = 'D';

--SELECT
--	*
--FROM Traces.dbo.TableFunction_BackupPendingPerType (@backupStart, @backupFinish, @backupType)

CREATE FUNCTION dbo.TableFunction_BackupPendingPerType (
	@backupStart DATETIME,
	@backupFinish DATETIME,
	@backupType CHAR(1)
)

RETURNS TABLE
AS 
RETURN (SELECT
			databases.name AS DatabaseName,
			databases.database_id,
			LastBackupFull.backup_start_date, 
			LastBackupFull.backup_finish_date,
			LastBackupFull.type
		FROM sys.databases
			OUTER APPLY (SELECT DISTINCT
							CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server, 
							backupset.database_name, 
							MIN(backupset.backup_start_date) OVER (PARTITION BY backupset.media_set_id, backupset.database_name) AS backup_start_date, 
							MAX(backupset.backup_finish_date) OVER (PARTITION BY backupset.media_set_id, backupset.database_name) AS backup_finish_date, 
							backupset.expiration_date, 
							backupset.type,
							CAST(ROUND(backupset.backup_size / 1024.0 / 1024.0, 2) AS DECIMAL(15,2)) AS backup_sizeMB, 
							CAST(ROUND(backupset.compressed_backup_size / 1024.0 / 1024.0, 2) AS DECIMAL(15,2)) AS compressed_backup_sizeMB,
							CAST(ROUND(backupset.backup_size / 1024.0 / 1024.0 / 1024.0, 2) AS DECIMAL(15,2)) AS backup_sizeGB, 
							CAST(ROUND(backupset.compressed_backup_size / 1024.0 / 1024.0 / 1024.0, 2) AS DECIMAL(15,2)) AS compressed_backup_sizeGB,
							CAST(ROUND(backupset.backup_size / 1024.0 / 1024.0 / 1024.0 / 1024.0, 2) AS DECIMAL(15,2)) AS backup_sizeTB, 
							CAST(ROUND(backupset.compressed_backup_size / 1024.0 / 1024.0 / 1024.0 / 1024.0, 2) AS DECIMAL(15,2)) AS compressed_backup_sizeTB,					
							backupset.name AS backupset_name, 
							backupset.description 					
						FROM msdb.dbo.backupset 							
							LEFT JOIN msdb.dbo.backupmediafamily 
								ON backupmediafamily.media_set_id = backupset.media_set_id 							
						WHERE 					
							1=1				
							AND backupset.database_name = databases.name	
							AND backupset.is_copy_only = 0
							AND backupset.is_snapshot = 0
							AND backupset.backup_finish_date BETWEEN @backupStart AND @backupFinish
							AND backupset.type = @backupType) AS LastBackupFull				
		WHERE
			databases.database_id > 4
			AND databases.source_database_id IS NULL
			AND databases.state = 0
			AND databases.is_read_only = 0
			AND LastBackupFull.database_name IS NULL
)