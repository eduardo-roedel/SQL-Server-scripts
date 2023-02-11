USE [msdb]
GO

/****** Object:  Job [TropicalPneus - AtualizacaoSistema - Bancos Movere]    Script Date: 01/12/2021 23:33:05 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 01/12/2021 23:33:05 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'TropicalPneus - AtualizacaoSistema - Bancos Movere', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Desenvolvido por Eduardo Roedel - PowerTuning - Ticket PWT20211109000000717. A rotina efetua um backup DIFF dos bancos de dados em questão e gera um Snapshot das bases para caso houver a necessidade de restaurar os banco de dados para o último estado antes da atualização, não quebrando a cadeia de backup.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'DBA_Operator', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute TropicalPneus]    Script Date: 01/12/2021 23:33:05 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute TropicalPneus', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE dbo.sp_start_job 
@job_name = N''TropicalPneus - AtualizacaoSistema - Banco TropicalPneus'';', 
		@database_name=N'msdb', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute TropicalPneusLog]    Script Date: 01/12/2021 23:33:05 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute TropicalPneusLog', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE dbo.sp_start_job 
@job_name = N''TropicalPneus - AtualizacaoSistema - Banco TropicalPneusLog'';', 
		@database_name=N'msdb', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute TropicalPneusNFe]    Script Date: 01/12/2021 23:33:05 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute TropicalPneusNFe', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE dbo.sp_start_job 
@job_name = N''TropicalPneus - AtualizacaoSistema - Banco TropicalPneusNFe'';', 
		@database_name=N'msdb', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
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

/****** Object:  Job [TropicalPneus - AtualizacaoSistema - Banco TropicalPneus]    Script Date: 01/12/2021 16:30:10 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 01/12/2021 16:30:10 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'TropicalPneus - AtualizacaoSistema - Banco TropicalPneus', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Desenvolvido por Eduardo Roedel - PowerTuning - Ticket PWT20211109000000717. A rotina efetua um backup DIFF do banco de dados em questão e gera um Snapshot da base para caso houver a necessidade de restaurar o banco de dados para o último estado antes da atualização, não quebrando a cadeia de backup.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'DBA_Operator', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute]    Script Date: 01/12/2021 16:30:10 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE dbo.stpExecuteBackupCreateSnapshot
@DatabaseName = N''TropicalPneus'',
@PathBackupDiff = ''\\192.168.0.20\backup_banco\Weekly'',
@PathSnapshot = N''K:\SnapshotsAtualizacao\TropicalPneusAtualizacao'';', 
		@database_name=N'Traces', 
		@output_file_name=N'K:\JobLogs\TropicalPneus - AtualizacaoSistema - Banco TropicalPneus_$(ESCAPE_SQUOTE(JOBID))_$(ESCAPE_SQUOTE(STEPID))_$(ESCAPE_SQUOTE(DATE))_$(ESCAPE_SQUOTE(TIME)).txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
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

/****** Object:  Job [TropicalPneus - AtualizacaoSistema - Banco TropicalPneusLog]    Script Date: 01/12/2021 16:30:18 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 01/12/2021 16:30:18 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'TropicalPneus - AtualizacaoSistema - Banco TropicalPneusLog', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Desenvolvido por Eduardo Roedel - PowerTuning - Ticket PWT20211109000000717. A rotina efetua um backup DIFF do banco de dados em questão e gera um Snapshot da base para caso houver a necessidade de restaurar o banco de dados para o último estado antes da atualização, não quebrando a cadeia de backup.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'DBA_Operator', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute]    Script Date: 01/12/2021 16:30:18 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE dbo.stpExecuteBackupCreateSnapshot
@DatabaseName = N''TropicalPneusLog'',
@PathBackupDiff = ''\\192.168.0.20\backup_banco\Weekly'',
@PathSnapshot = N''D:\SnapshotsAtualizacao\TropicalPneusLogAtualizacao'';', 
		@database_name=N'Traces', 
		@output_file_name=N'K:\JobLogs\TropicalPneus - AtualizacaoSistema - Banco TropicalPneusLog_$(ESCAPE_SQUOTE(JOBID))_$(ESCAPE_SQUOTE(STEPID))_$(ESCAPE_SQUOTE(DATE))_$(ESCAPE_SQUOTE(TIME)).txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
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

/****** Object:  Job [TropicalPneus - AtualizacaoSistema - Banco TropicalPneusNFe]    Script Date: 01/12/2021 16:30:27 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 01/12/2021 16:30:27 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'TropicalPneus - AtualizacaoSistema - Banco TropicalPneusNFe', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Desenvolvido por Eduardo Roedel - PowerTuning - Ticket PWT20211109000000717. A rotina efetua um backup DIFF do banco de dados em questão e gera um Snapshot da base para caso houver a necessidade de restaurar o banco de dados para o último estado antes da atualização, não quebrando a cadeia de backup.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'DBA_Operator', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute]    Script Date: 01/12/2021 16:30:27 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE dbo.stpExecuteBackupCreateSnapshot
@DatabaseName = N''TropicalPneusNFe'',
@PathBackupDiff = ''\\192.168.0.20\backup_banco\Weekly'',
@PathSnapshot = N''D:\SnapshotsAtualizacao\TropicalPneusNFeAtualizacao'';', 
		@database_name=N'Traces', 
		@output_file_name=N'K:\JobLogs\TropicalPneus - AtualizacaoSistema - Banco TropicalPneusNFe_$(ESCAPE_SQUOTE(JOBID))_$(ESCAPE_SQUOTE(STEPID))_$(ESCAPE_SQUOTE(DATE))_$(ESCAPE_SQUOTE(TIME)).txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
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

/****** Object:  Job [TropicalPneus - AtualizacaoSistemaRollback - Banco TropicalPneus]    Script Date: 01/12/2021 16:30:34 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 01/12/2021 16:30:35 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'TropicalPneus - AtualizacaoSistemaRollback - Banco TropicalPneus', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Desenvolvido por Eduardo Roedel - PowerTuning - Ticket PWT20211109000000717. A rotina altera o banco de dados para SINGLE_USER e restaura o banco de dados sobrescrevendo, utilizando o snapshot criado.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute]    Script Date: 01/12/2021 16:30:35 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE dbo.stpExecuteRestoreDatabaseFromSnapshot
@DatabaseName = N''TropicalPneus'',
@DatabaseNameSnapshot = N''TropicalPneusAtualizacao'';', 
		@database_name=N'Traces', 
		@output_file_name=N'K:\JobLogs\TropicalPneus - AtualizacaoSistemaRollback - Banco TropicalPneus_$(ESCAPE_SQUOTE(JOBID))_$(ESCAPE_SQUOTE(STEPID))_$(ESCAPE_SQUOTE(DATE))_$(ESCAPE_SQUOTE(TIME)).txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
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

/****** Object:  Job [TropicalPneus - AtualizacaoSistemaRollback - Banco TropicalPneusLog]    Script Date: 01/12/2021 16:30:41 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 01/12/2021 16:30:41 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'TropicalPneus - AtualizacaoSistemaRollback - Banco TropicalPneusLog', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Desenvolvido por Eduardo Roedel - PowerTuning - Ticket PWT20211109000000717. A rotina altera o banco de dados para SINGLE_USER e restaura o banco de dados sobrescrevendo, utilizando o snapshot criado.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute]    Script Date: 01/12/2021 16:30:41 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE dbo.stpExecuteRestoreDatabaseFromSnapshot
@DatabaseName = N''TropicalPneusLog'',
@DatabaseNameSnapshot = N''TropicalPneusLogAtualizacao'';', 
		@database_name=N'Traces', 
		@output_file_name=N'K:\JobLogs\TropicalPneus - AtualizacaoSistemaRollback - Banco TropicalPneusLog_$(ESCAPE_SQUOTE(JOBID))_$(ESCAPE_SQUOTE(STEPID))_$(ESCAPE_SQUOTE(DATE))_$(ESCAPE_SQUOTE(TIME)).txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
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

/****** Object:  Job [TropicalPneus - AtualizacaoSistemaRollback - Banco TropicalPneusNFe]    Script Date: 01/12/2021 16:30:53 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 01/12/2021 16:30:53 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'TropicalPneus - AtualizacaoSistemaRollback - Banco TropicalPneusNFe', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Desenvolvido por Eduardo Roedel - PowerTuning - Ticket PWT20211109000000717. A rotina altera o banco de dados para SINGLE_USER e restaura o banco de dados sobrescrevendo, utilizando o snapshot criado.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute]    Script Date: 01/12/2021 16:30:53 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE dbo.stpExecuteRestoreDatabaseFromSnapshot
@DatabaseName = N''TropicalPneusNFe'',
@DatabaseNameSnapshot = N''TropicalPneusNFeAtualizacao'';', 
		@database_name=N'Traces', 
		@output_file_name=N'K:\JobLogs\TropicalPneus - AtualizacaoSistemaRollback - Banco TropicalPneusNFe_$(ESCAPE_SQUOTE(JOBID))_$(ESCAPE_SQUOTE(STEPID))_$(ESCAPE_SQUOTE(DATE))_$(ESCAPE_SQUOTE(TIME)).txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO
EXECUTE dbo.sp_start_job 
@job_name = N'TropicalPneus - AtualizacaoSistema - Banco TropicalPneus';


USE [Traces];
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

USE [Traces];
GO

CREATE PROCEDURE stpExecuteRestoreDatabaseFromSnapshot (
	@DatabaseName SYSNAME,
	@DatabaseNameSnapshot SYSNAME
)
AS
SET NOCOUNT ON;
	BEGIN
		DECLARE @SnapshotCommand NVARCHAR(4000);

		IF EXISTS (SELECT
						databases.name
					FROM sys.databases
					WHERE
						databases.name = N'TropicalPneusBITeste')
			BEGIN
				SET @SnapshotCommand = N'DROP DATABASE [TropicalPneusBITeste]';
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

		EXECUTE sp_executesql @stmt = @SnapshotCommand;

		IF @DatabaseName = N'TropicalPneus'
			BEGIN
				EXECUTE msdb.dbo.sp_start_job @job_name = N'DBA - Recreate Database Snapshot';	
			END
	END