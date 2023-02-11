USE [database];
GO

--Cria��o da estrutura
CREATE TABLE dbo.JobMonitoring (
	Id SMALLINT IDENTITY(1, 1) PRIMARY KEY,
	job_name SYSNAME,
	TimeLimitSecond INT NOT NULL,
	session_id SMALLINT,
	Active BIT NOT NULL DEFAULT 1
) WITH (DATA_COMPRESSION = PAGE);

CREATE UNIQUE INDEX SK01_JobMonitoring ON dbo.JobMonitoring (job_name) WITH (DATA_COMPRESSION = PAGE);

--Insert dos jobs que se deseja monitorar.
INSERT INTO dbo.JobMonitoring (job_name, TimeLimitSecond)
SELECT 
	sysjobs.name, 
	600 AS TimeLimitSecond
FROM msdb.dbo.sysjobs 
WHERE 
	sysjobs.name IN ('', '');

----Step inicial do tipo T-SQL para CADA job que se deseja monitorar.
DECLARE @job_id UNIQUEIDENTIFIER;
DECLARE @JobName SYSNAME;
DECLARE @session_id SMALLINT;

SELECT 
	@JobName = sysjobs.name,	
	@job_id = sysjobs.job_id,
	@session_id = @@SPID
FROM msdb.dbo.sysjobs 
WHERE 
	sysjobs.job_id = $(ESCAPE_NONE(JOBID));

UPDATE dbo.JobMonitoring 
SET JobMonitoring.session_id = @session_id 
WHERE 
	JobMonitoring.job_name = @JobName;

-----

GO

-- View para ser consumida no powerBi ou por onde for necess�rio.

USE [database];
GO

CREATE VIEW dbo.vw_JobMonitoring 
AS

SELECT 
	JobMonitoring.job_name AS JobName,
	JobMonitoring.TimeLimitSecond,	
	LimitExecutionTime.FormattedLimitExecution,
	ISNULL(DATEDIFF_BIG(SECOND, dm_exec_requests.start_time, ListDate.ActualDate), 0) AS ExecutingTimeSecond,
	CASE
		WHEN ExecutionTime.FormattedExecution = '.::' THEN ''
			ELSE ExecutionTime.FormattedExecution
	END AS FormattedExecution,	
	ISNULL(dm_exec_sessions.last_request_start_time, '') AS run_requested_date,
	ISNULL(JobMonitoring.session_id, 0) AS session_id,	
	ISNULL(dm_exec_sessions.status, 'Not running') AS StatusProcess,
	ISNULL(dm_exec_requests.blocking_session_id, 0) AS blocking_session_id,
	ISNULL(dm_exec_requests.last_wait_type, '') AS last_wait_type,	
	ISNULL(jobhistory.LastStatusJobHistory, '') AS LastStatusJobHistory
FROM dbo.JobMonitoring 
	CROSS APPLY (SELECT TOP 1
						GETDATE() AS ActualDate) AS ListDate
	INNER JOIN msdb.dbo.sysjobs
		ON sysjobs.name = JobMonitoring.job_name
	LEFT JOIN sys.dm_exec_sessions		 
		ON dm_exec_sessions.session_id = JobMonitoring.session_id
	LEFT JOIN sys.dm_exec_requests
		ON dm_exec_requests.session_id = dm_exec_sessions.session_id
	OUTER APPLY (SELECT TOP 1
						CASE sysjobhistory.run_status
							WHEN 0 THEN 'Failed'
							WHEN 1 THEN 'Succeeded'
							WHEN 2 THEN 'Retry'
							WHEN 3 THEN 'Canceled'
							WHEN 4 THEN 'In Progress'
						END AS LastStatusJobHistory							
					FROM msdb.dbo.sysjobhistory
						CROSS APPLY (SELECT
											MAX(jobhistoryMax.instance_id) AS instance_idMax
										FROM msdb.dbo.sysjobhistory AS jobhistoryMax
										WHERE 
											jobhistoryMax.job_id = sysjobs.job_id) AS jobhistoryMax
					WHERE				
						sysjobhistory.job_id = sysjobs.job_id
						AND sysjobhistory.instance_id = jobhistoryMax.instance_idMax) AS jobhistory
	OUTER APPLY (SELECT
						TimeFormat.*,
						CONCAT(TimeFormat.QtdDay, '.', RIGHT((REPLICATE('0', 2) + CAST(TimeFormat.QtdHour AS VARCHAR)), 2), ':', RIGHT((REPLICATE('0', 2) + CAST(TimeFormat.QtdMinute AS VARCHAR)), 2), ':', RIGHT((REPLICATE('0', 2) + CAST(TimeFormat.QtdSecond AS VARCHAR)), 2)) AS FormattedExecution
					FROM (SELECT						
								TableName.ColumnName,
								CAST(ROUND(TableName.ColumnName / 60 / 60 / 24.0, 0, 1) AS INT) AS QtdDay,
								CAST(ROUND(TableName.ColumnName / 60 / 60.0, 0, 1) % 24 AS INT) AS QtdHour,
								CAST(ROUND(TableName.ColumnName / 60, 0, 1) % 60 AS INT) AS QtdMinute,
								CAST(ROUND(TableName.ColumnName, 0, 1) % 60 AS INT) AS QtdSecond						
							FROM (VALUES (DATEDIFF_BIG(SECOND, dm_exec_requests.start_time, ListDate.ActualDate))) AS TableName(ColumnName)) AS TimeFormat) AS ExecutionTime
	OUTER APPLY (SELECT
						TimeFormat.*,
						CONCAT(TimeFormat.QtdDay, '.', RIGHT((REPLICATE('0', 2) + CAST(TimeFormat.QtdHour AS VARCHAR)), 2), ':', RIGHT((REPLICATE('0', 2) + CAST(TimeFormat.QtdMinute AS VARCHAR)), 2), ':', RIGHT((REPLICATE('0', 2) + CAST(TimeFormat.QtdSecond AS VARCHAR)), 2)) AS FormattedLimitExecution
					FROM (SELECT						
								TableName.ColumnName,
								CAST(ROUND(TableName.ColumnName / 60 / 60 / 24.0, 0, 1) AS INT) AS QtdDay,
								CAST(ROUND(TableName.ColumnName / 60 / 60.0, 0, 1) % 24 AS INT) AS QtdHour,
								CAST(ROUND(TableName.ColumnName / 60, 0, 1) % 60 AS INT) AS QtdMinute,
								CAST(ROUND(TableName.ColumnName, 0, 1) % 60 AS INT) AS QtdSecond						
							FROM (VALUES (JobMonitoring.TimeLimitSecond)) AS TableName(ColumnName)) AS TimeFormat) AS LimitExecutionTime
WHERE	
	JobMonitoring.Active = 1;
	
GO

--Procedures que fazem o envio do e-mail
USE [database];
GO

CREATE PROCEDURE [dbo].[stpExporta_Tabela_HTML_Output]
    @TableName VARCHAR(MAX),
    @DefaultStyle BIT = 1,
    @Align VARCHAR(10) = 'left',
    @OrderBy VARCHAR(MAX) = '',
	@ColorHeadBackground VARCHAR(30) = '#024999',
	@ColorHeadFont VARCHAR(30) = '#ddd',
    @OutputHTML VARCHAR(MAX) OUTPUT 
AS
BEGIN
	--https://www.dirceuresende.com/blog/sql-server-como-enviar-o-conteudo-de-uma-tabela-ou-query-no-corpo-do-e-mail-como-html/
    SET NOCOUNT ON;
    
    DECLARE @query NVARCHAR(MAX);
    DECLARE @Database SYSNAME;
    DECLARE @Nome_Tabela SYSNAME;
	DECLARE @contadorColuna INT; 
    DECLARE @totalColunas INT;
    DECLARE @nomeColuna SYSNAME;
    DECLARE @tipoColuna SYSNAME;
	DECLARE @saida VARCHAR(MAX);
    
    IF (LEFT(@TableName, 1) = '#')
		BEGIN
		    SET @Database = 'tempdb.'
		    SET @Nome_Tabela = @TableName
		END
			ELSE 
				BEGIN
					SET @Database = LEFT(@TableName, CHARINDEX('.', @TableName))
					SET @Nome_Tabela = SUBSTRING(@TableName, LEN(@TableName) - CHARINDEX('.', REVERSE(@TableName)) + 2, LEN(@TableName))
				END

    
    SET @query = '
    SELECT columns.ordinal_position, columns.column_name, columns.data_type, columns.character_maximum_length, columns.numeric_precision, columns.numeric_scale
    FROM ' + @Database + 'information_schema.columns 
    WHERE columns.table_name = ''' + @Nome_Tabela + '''
    ORDER BY columns.ordinal_position'    
    
    IF (OBJECT_ID('tempdb..#Colunas') IS NOT NULL) 
		BEGIN
			DROP TABLE #Colunas
		END

    CREATE TABLE #Colunas (
        ORDINAL_POSITION INT, 
        COLUMN_NAME SYSNAME, 
        DATA_TYPE NVARCHAR(128), 
        CHARACTER_MAXIMUM_LENGTH INT,
        NUMERIC_PRECISION TINYINT, 
        NUMERIC_SCALE INT) WITH (DATA_COMPRESSION = PAGE)

    INSERT INTO #Colunas
    EXEC(@query)    
    
    IF (@DefaultStyle = 1)
		BEGIN    
			SET @OutputHTML = '<html>
								<head>
								    <title>Titulo</title>
								    <style type="text/css">
								        table { padding:0; border-spacing: 0; border-collapse: collapse; }
								        thead { background: ' + @ColorHeadBackground + '; border: 1px solid ' + @ColorHeadFont + '; }
								        th { padding: 10px; font-weight: bold; border: 1px solid #000; color: #fff; }
								        tr { padding: 0; }
								        td { padding: 5px; border: 1px solid #000; margin:0; text-align:' + @Align + '; }
								    </style>
								</head>'    
    END    
    
    SET @OutputHTML = ISNULL(@OutputHTML, '') + '
	<table>
	    <thead>
	        <tr>'

    -- Cabe�alho da tabela
    SET @contadorColuna = 1;
    SET @totalColunas = (SELECT COUNT(*) FROM #Colunas);    

    WHILE (@contadorColuna <= @totalColunas)
		BEGIN
			SELECT 
				@nomeColuna = COLUMN_NAME
			FROM #Colunas
			WHERE
				ORDINAL_POSITION = @contadorColuna;

			SET @OutputHTML = ISNULL(@OutputHTML, '') + '
			    <th>' + @nomeColuna + '</th>'

			SET @contadorColuna = @contadorColuna + 1
    END

    SET @OutputHTML = ISNULL(@OutputHTML, '') + '
        </tr>
    </thead>
    <tbody>'
    
    -- Conte�do da tabela  
    SET @query = '
	SELECT @saida = (
	    SELECT '

    SET @contadorColuna = 1

    WHILE (@contadorColuna <= @totalColunas)
		BEGIN
		    SELECT 
		        @nomeColuna = COLUMN_NAME,
		        @tipoColuna = DATA_TYPE
		    FROM 
		        #Colunas
		    WHERE 
		        ORDINAL_POSITION = @contadorColuna;

		    IF (@tipoColuna IN ('int', 'bigint', 'float', 'numeric', 'decimal', 'bit', 'tinyint', 'smallint', 'integer'))
				BEGIN		    
					SET @query = @query + '
					ISNULL(CAST([' + @nomeColuna + '] AS VARCHAR(MAX)), '''') AS [td]'		
				END
					ELSE
						BEGIN		    
							SET @query = @query + '
							ISNULL([' + @nomeColuna + '], '''') AS [td]'		
						END
		
		    
		    IF (@contadorColuna < @totalColunas)
		        SET @query = @query + ','		    
				SET @contadorColuna = @contadorColuna + 1
		END

    SET @query = @query + '
	FROM ' + @TableName + (CASE WHEN ISNULL(@OrderBy, '') = '' THEN '' ELSE ' 
	ORDER BY ' END) + @OrderBy + '
	FOR XML RAW(''tr''), Elements
	)';    
    
    EXEC tempdb.sys.sp_executesql
        @query,
        N'@saida NVARCHAR(MAX) OUTPUT',
        @saida OUTPUT

    -- Identa��o
    SET @saida = REPLACE(@saida, '<tr>', '
        <tr>');

    SET @saida = REPLACE(@saida, '<td>', '
            <td>');

    SET @saida = REPLACE(@saida, '</tr>', '
        </tr>');

    SET @OutputHTML = ISNULL(@OutputHTML, '') + @saida;
    
    SET @OutputHTML = ISNULL(@OutputHTML, '') + '
    </tbody>
	</table>';    
	
END

USE [database];
GO

CREATE PROCEDURE dbo.stp_JobMonitoring
	@profile_name SYSNAME,
	@recipients VARCHAR(MAX),
	@copy_recipients VARCHAR(MAX),
	@subject NVARCHAR(255),
	@importance VARCHAR(6),
	@Align VARCHAR(10) = 'left',
    @OrderBy VARCHAR(MAX) = '',
	@ColorHeadBackground VARCHAR(30) = '#236BAE',
	@ColorHeadFont VARCHAR(30) = '#F2F2F2',
	@HtmlHeaderMessage VARCHAR(MAX),
	@HtmlFooterMessage VARCHAR(MAX)
AS
SET NOCOUNT ON;
BEGIN
	
	DECLARE @HTML VARCHAR(MAX);
	DECLARE @msg VARCHAR(MAX)

	IF @importance IS NULL OR @importance NOT IN ('Low', 'Normal', 'High')
		BEGIN
			SET @msg = '@importance value is invalid or was not informed. Sending as ''Normal''';
			RAISERROR (@msg, 0, 1);
			SET @importance = 'Normal';
		END

	IF OBJECT_ID('tempdb..##JobMonitoring') IS NOT NULL
		BEGIN
			DROP TABLE ##JobMonitoring;
		END	

	SELECT
		*
	INTO ##JobMonitoring
	FROM dbo.vw_JobMonitoring
	WHERE 
		vw_JobMonitoring.ExecutingTimeSecond >= vw_JobMonitoring.TimeLimitSecond

	IF (SELECT COUNT(*) FROM ##JobMonitoring) > 0
		BEGIN

			EXECUTE dbo.stpExporta_Tabela_HTML_Output
				@TableName = '##JobMonitoring',
				@Align = @Align,
				@OrderBy = @OrderBy,
				@ColorHeadBackground = @ColorHeadBackground,
				@ColorHeadFont = @ColorHeadFont,
				@OutputHTML = @HTML OUTPUT

			SET @HTML = CONCAT(ISNULL(@HtmlHeaderMessage, ''), @HTML, ISNULL(@HtmlFooterMessage,''));	

			EXECUTE msdb.dbo.sp_send_dbmail 
				@profile_name = @profile_name, 
				@recipients = @recipients,
				@copy_recipients = @copy_recipients,
				@subject = @subject, 
				@body_format = 'HTML', 
				@body = @HTML, 
				@importance = @importance;
		END

	DROP TABLE ##JobMonitoring;

END

GO

--Jobs respons�veis pelo envio do monitoramento

USE [msdb]
GO

/****** Object:  Job [Job Monitor]    Script Date: 1/16/2023 11:40:43 AM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 1/16/2023 11:40:43 AM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Job Monitor', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Job Monitor]    Script Date: 1/16/2023 11:40:43 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Job Monitor', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE @profile_name SYSNAME;
DECLARE @recipients VARCHAR(MAX);
DECLARE @copy_recipients VARCHAR(MAX);
DECLARE @subject NVARCHAR(255);
DECLARE @importance VARCHAR(6);
DECLARE @HtmlHeaderMessage VARCHAR(MAX);
DECLARE @HtmlFooterMessage VARCHAR(MAX);

SET @profile_name = N''Alert'';
SET @recipients = '''';
SET @copy_recipients =  '''';
SET @subject = N''Alerta: '' + QUOTENAME(@@SERVERNAME) + N'' - Jobs executando a mais tempo que esperado'';
SET @importance = ''Normal'';
SET @HtmlHeaderMessage = CONCAT(''<h7>Ol�,</h7>
<p>Abaixo segue lista de jobs que ultrapassaram as janelas de execu��o estipuladas: </p><br/>'', ''<p> Auditado na inst�ncia '', @@SERVERNAME, '' em '', CAST(GETDATE() AS VARCHAR(113)), ''.</p>'');
SET @HtmlFooterMessage = ''<br/><br/><img src="https://www.kkk.com.br/portal/wp-content/uploads/2016/08/logo__portal-e1472042151793.png"/>'';

EXECUTE dbo.stp_JobMonitoring
	@profile_name = @profile_name,
	@recipients = @recipients,
	@copy_recipients = @copy_recipients,
	@subject = @subject,
	@importance = @importance,
	@HtmlHeaderMessage = @HtmlHeaderMessage,
	@HtmlFooterMessage = @HtmlFooterMessage;', 
		@database_name=N'database', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Clean history]    Script Date: 1/16/2023 11:40:43 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Clean history', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE @job_name SYSNAME;
DECLARE @step_name SYSNAME;

SET @job_name = N''sp_purge_jobhistory'';
SET @step_name = N''Clean history  - Job Monitor'';

EXECUTE msdb.dbo.sp_start_job
@job_name = @job_name,
@step_name = @step_name;
', 
		@database_name=N'msdb', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'10s', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=2, 
		@freq_subday_interval=10, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20230116, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

USE [msdb]
GO

/****** Object:  Job [sp_purge_jobhistory]    Script Date: 1/16/2023 12:38:25 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 1/16/2023 12:38:25 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'sp_purge_jobhistory', 
		@enabled=1, 
		@notify_level_eventlog=2, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Source: https://ola.hallengren.com', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [sp_purge_jobhistory]    Script Date: 1/16/2023 12:38:25 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'sp_purge_jobhistory', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE @StartDate DATETIME = (SELECT CAST(MIN(dbo.agent_datetime(sysjobhistory.run_date, sysjobhistory.run_time)) AS DATE) AS MinRunDatetime from dbo.sysjobhistory);
DECLARE @DayToRetain SMALLINT = 90;
DECLARE @oldest_date DATETIME = @StartDate;
DECLARE @Difference INT = DATEDIFF(DAY, @StartDate, DATEADD(DAY, - @DayToRetain, GETDATE()));

WHILE @Difference > 0
	BEGIN
		SET @oldest_date = DATEADD(DAY, 1, @oldest_date);
		
		EXECUTE dbo.sp_purge_jobhistory @oldest_date = @oldest_date;

		SET @Difference = @Difference - 1;		
	END', 
		@database_name=N'msdb', 		 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Clean history  - Job Monitor]    Script Date: 1/16/2023 12:38:25 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Clean history  - Job Monitor', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE @CleanupDate DATETIME;
DECLARE @job_name SYSNAME;

SET @CleanupDate = DATEADD(DAY, -1, GETDATE());
SET @job_name = N'' - Job Monitor'';

EXECUTE dbo.sp_purge_jobhistory 
@job_name = @job_name,
@oldest_date = @CleanupDate', 
		@database_name=N'msdb', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'1x month', 
		@enabled=1, 
		@freq_type=32, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=1, 
		@freq_relative_interval=1, 
		@freq_recurrence_factor=1, 
		@active_start_date=20230116, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO