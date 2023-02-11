DECLARE @msg NVARCHAR(4000);
DECLARE @ActualInstanceName SYSNAME;
DECLARE @NewInstanceName SYSNAME;

SET @ActualInstanceName = @@SERVERNAME;
SET @NewInstanceName = (SELECT 
							CASE
								WHEN CAST(SERVERPROPERTY('InstanceName') AS NVARCHAR) IS NULL THEN CAST(SERVERPROPERTY('Machinename') AS NVARCHAR)
									ELSE CAST(SERVERPROPERTY('Machinename') AS NVARCHAR) + N'\' + CAST(SERVERPROPERTY('InstanceName') AS NVARCHAR)
						END);
SET @msg = N'Renaming instance from ' + @ActualInstanceName + N' to ' + @NewInstanceName + N' ...';
	
RAISERROR(@msg, 0, 1);

EXECUTE sp_dropserver @ActualInstanceName;
EXECUTE sp_addserver @NewInstanceName, 'local';