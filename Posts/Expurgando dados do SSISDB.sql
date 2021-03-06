CREATE OR ALTER PROCEDURE [dbo].[Procedure_HousekeepingSSISDB] (
	@QtdDeletedRowToDeletePerCycle INT = 1000) AS 

SET NOCOUNT ON;

	BEGIN

	/*
		Script name: Purge SSIS Catalog log tables
		Author: Tim Mitchell (www.TimMitchell.net)
		https://www.timmitchell.net/post/2018/12/30/clean-up-the-ssis-catalog/
		Date: 12/19/2018
	
		Purpose: This script will remove most of the operational information from the SSIS catalog. The 
					internal.operations and internal.executions tables, as well as their dependencies, 
					will be purged of all data with an operation created_time value older than the number
					of days specified in the RETENTION_WINDOW setting of the SSIS catalog.
					
			Note that this script was created using SQL Server 2017 (14.0.3048.4). Depending on the SQL Server
					version, the table and/or column names may be different.
	
		Modified by: Eduardo Roedel
		Date 02/21/2021	
	*/
	
	/*
		Query the SSIS catalog database for the retention settings
	*/
	
	IF OBJECT_ID('dbo.ErrorControlScript') IS NULL
		BEGIN
			CREATE TABLE [dbo].[ErrorControlScript](
				Id INT IDENTITY(1,1) PRIMARY KEY,
				Script SYSNAME,
				ErrorNumber INT,
				ErrorSeverity INT,
				ErrorState INT,
				ErrorProcedure NVARCHAR(128),
				ErrorLine INT,
				ErrorMessage NVARCHAR(4000)
			) WITH (DATA_COMPRESSION = PAGE);
		END
	
	DECLARE @enable_purge BIT = (SELECT
									CONVERT(BIT, catalog_properties.property_value) AS property_value
								FROM SSISDB.[catalog].catalog_properties
								WHERE
									catalog_properties.property_name = N'OPERATION_CLEANUP_ENABLED');
	
	DECLARE @retention_period_days SMALLINT = (SELECT 
													CONVERT(INT, catalog_properties.property_value) AS property_value
												FROM SSISDB.[catalog].catalog_properties
												WHERE
													catalog_properties.property_name = N'RETENTION_WINDOW');
	
	DECLARE @created_date DATETIME = DATEADD(DAY, 0 - @retention_period_days, GETDATE());
	DECLARE @QtdDeletedRow BIGINT;
	DECLARE @execution_id BIGINT;
	DECLARE @msg VARCHAR(500);
	DECLARE @CounterRow BIGINT;
	DECLARE @SQLStatement NVARCHAR(4000);
	DECLARE @Script SYSNAME = N'Procedure_HousekeepingSSISDB';
	
	
	/*
		If purge is disabled or the retention period is not greater than 0, skip the remaining tasks
	  by turning on NOEXEC.
	*/
	
	IF NOT (@enable_purge = 1 AND @retention_period_days > 0)
		SET NOEXEC ON 
	
	/*
		Get the working list of execution IDs. This will be the list of IDs we use for the
		delete operation for each table.
	*/
	
	IF (OBJECT_ID('tempdb..##executionList') IS NOT NULL)
		BEGIN
			DROP TABLE ##executionList
		END;
	
	CREATE TABLE ##executionList (
		execution_id BIGINT PRIMARY KEY,
		isDeleted BIT DEFAULT 0,
		startTime DATETIME,
		endTime DATETIME,
		Qtd_event_message_context BIGINT,
		Qtd_event_messages BIGINT,
		Qtd_executable_statistics BIGINT,
		Qtd_execution_data_statistics BIGINT,
		Qtd_execution_component_phases BIGINT,
		Qtd_execution_data_taps BIGINT,
		Qtd_execution_parameter_values BIGINT,
		Qtd_execution_property_override_values BIGINT,
		Qtd_executions BIGINT,
		Qtd_operation_messages BIGINT,
		Qtd_extended_operation_info BIGINT,
		Qtd_operation_os_sys_info BIGINT,
		Qtd_validations BIGINT,
		Qtd_operation_permissions BIGINT,
		Qtd_operations BIGINT	
	) WITH (DATA_COMPRESSION = PAGE);
	
	INSERT INTO ##executionList WITH (TABLOCK) (execution_id)
	SELECT 
		executions.execution_id
	FROM SSISDB.catalog.executions 
	WHERE 
		CAST(executions.created_time AS DATETIME) < @created_date
	
		
	WHILE (SELECT COUNT(*) FROM ##executionList WHERE ##executionList.isDeleted = 0) > 0
		BEGIN
		
			SET @execution_id = (SELECT TOP 1 
									##executionList.execution_id 
								FROM ##executionList
								WHERE 
									##executionList.isDeleted = 0) ;
	
			UPDATE ##executionList 
			SET startTime = GETDATE() 
			WHERE 
				##executionList.execution_id = @execution_id;
		
			/***************************************************
				internal.executions and its dependencies
			***************************************************/			
				
				/*
					internal.event_message_context
				*/
			
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(event_message_context.context_id) AS Qtd
										FROM SSISDB.internal.event_message_context
											INNER JOIN SSISDB.internal.event_messages
												ON event_messages.event_message_id = event_message_context.event_message_id
											INNER JOIN ##executionList
												ON ##executionList.execution_id = event_messages.operation_id
										WHERE
											##executionList.execution_id = @execution_id);
	
					UPDATE ##executionList 
					SET ##executionList.Qtd_event_message_context = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
	
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 				
									
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ')				
																	event_message_context.context_id
																FROM SSISDB.internal.event_message_context
																	INNER JOIN SSISDB.internal.event_messages
																		ON event_messages.event_message_id = event_message_context.event_message_id
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = event_messages.operation_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)							
								
								DELETE event_message_context
									FROM SSISDB.internal.event_message_context
										INNER JOIN CTEDeleteRow
											ON CTEDeleteRow.context_id = event_message_context.context_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
	
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.event_message_context.');																			
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;
	
								UPDATE ##executionList 
								SET ##executionList.Qtd_event_message_context = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
								
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT  N'The transaction is in an uncommittable state. Rolling back transaction.';
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH		
			
				/*
					internal.event_messages
				*/
	
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(event_messages.event_message_id) AS Qtd
										FROM SSISDB.internal.event_messages
											INNER JOIN ##executionList
												ON ##executionList.execution_id = event_messages.operation_id
										WHERE
											##executionList.execution_id = @execution_id);
	
					UPDATE ##executionList 
					SET ##executionList.Qtd_event_messages = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
	
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 				
							
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	event_messages.event_message_id
																FROM SSISDB.internal.event_messages												
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = event_messages.operation_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)
																	
								DELETE event_messages
									FROM SSISDB.internal.event_messages
										INNER JOIN CTEDeleteRow
											ON CTEDeleteRow.event_message_id = event_messages.event_message_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.event_messages.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;	
								
								UPDATE ##executionList 
								SET ##executionList.Qtd_event_messages = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
	
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT  N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';
							COMMIT TRANSACTION;     
						END;
					
				END CATCH
			
				/*
					internal.executable_statistics 
				*/
	
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(executable_statistics.statistics_id) AS Qtd
										FROM SSISDB.internal.executable_statistics
											INNER JOIN ##executionList
												ON ##executionList.execution_id = executable_statistics.execution_id
										WHERE
											executable_statistics.execution_id = @execution_id);			
	
					UPDATE ##executionList 
					SET ##executionList.Qtd_executable_statistics = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
	
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 				
							
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	executable_statistics.statistics_id
																FROM SSISDB.internal.executable_statistics												
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = executable_statistics.execution_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
																		
								DELETE executable_statistics
									FROM SSISDB.internal.executable_statistics
										INNER JOIN CTEDeleteRow
											ON CTEDeleteRow.statistics_id = executable_statistics.statistics_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.executable_statistics.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;	
	
								UPDATE ##executionList 
								SET ##executionList.Qtd_executable_statistics = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
								
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT  N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH		
			
				/*
					internal.execution_data_statistics is one of the larger tables. Break up the delete to avoid
					log size explosion.
				*/
	
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(execution_data_statistics.data_stats_id) AS Qtd
										FROM SSISDB.internal.execution_data_statistics
											INNER JOIN ##executionList
												ON ##executionList.execution_id = execution_data_statistics.execution_id
										WHERE
											execution_data_statistics.execution_id = @execution_id);	
										
					UPDATE ##executionList 
					SET ##executionList.Qtd_execution_data_statistics = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
	
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 	
							
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	execution_data_statistics.data_stats_id
																FROM SSISDB.internal.execution_data_statistics												
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = execution_data_statistics.execution_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE execution_data_statistics
									FROM SSISDB.internal.execution_data_statistics
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.data_stats_id = executable_statistics.data_stats_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.execution_data_statistics.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;	
								
								UPDATE ##executionList 
								SET ##executionList.Qtd_execution_data_statistics = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
	
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT  N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH		
	
				/*
					internal.execution_component_phases is one of the larger tables. Break up the delete to avoid
					log size explosion.
				*/
	
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(execution_component_phases.phase_stats_id) AS Qtd
										FROM SSISDB.internal.execution_component_phases
											INNER JOIN ##executionList
												ON ##executionList.execution_id = execution_component_phases.execution_id
										WHERE
											execution_component_phases.execution_id = @execution_id);	
										
					UPDATE ##executionList 
					SET ##executionList.Qtd_execution_component_phases = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
										
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 			
								
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	execution_component_phases.phase_stats_id
																FROM SSISDB.internal.execution_data_statistics												
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = execution_data_statistics.execution_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE execution_component_phases
									FROM SSISDB.internal.execution_component_phases
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.phase_stats_id = executable_statistics.phase_stats_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.execution_component_phases.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;	
	
								UPDATE ##executionList 
								SET ##executionList.Qtd_execution_component_phases = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
								
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT  N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH		
		
				/*
					internal.execution_data_taps
				*/
				   
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(execution_data_taps.data_tap_id) AS Qtd
										FROM SSISDB.internal.execution_data_taps
											INNER JOIN ##executionList
												ON ##executionList.execution_id = execution_data_taps.execution_id
										WHERE
											##executionList.execution_id = @execution_id);		
										
					UPDATE ##executionList 
					SET ##executionList.Qtd_execution_data_taps = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
										
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 			
								
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	execution_data_taps.data_tap_id
																FROM SSISDB.internal.execution_data_taps												
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = execution_data_taps.execution_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE execution_data_taps
									FROM SSISDB.internal.execution_data_taps
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.data_tap_id = executable_statistics.data_tap_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.execution_data_taps.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;
								
								UPDATE ##executionList 
								SET ##executionList.Qtd_execution_data_taps = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
								
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH	
			
				/*
					internal.execution_parameter_values is one of the larger tables. Break up the delete to avoid
					log size explosion.
				*/
			
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(execution_parameter_values.execution_parameter_id) AS Qtd
										FROM SSISDB.internal.execution_parameter_values
											INNER JOIN ##executionList
												ON ##executionList.execution_id = execution_parameter_values.execution_id
										WHERE
											execution_parameter_values.execution_id = @execution_id);	
										
					UPDATE ##executionList 
					SET ##executionList.Qtd_execution_parameter_values = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
										
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 			
														
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	execution_parameter_values.execution_parameter_id
																FROM SSISDB.internal.execution_parameter_values												
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = execution_parameter_values.execution_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE execution_parameter_values
									FROM SSISDB.internal.execution_parameter_values
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.execution_parameter_id = execution_parameter_values.execution_parameter_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.execution_parameter_values.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;	
								
								UPDATE ##executionList 
								SET ##executionList.Qtd_execution_parameter_values = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
	
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH	
					
				/*
					internal.execution_property_override_values
				*/
	
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(execution_property_override_values.property_id) AS Qtd
										FROM SSISDB.internal.execution_property_override_values
											INNER JOIN ##executionList
												ON ##executionList.execution_id = execution_property_override_values.execution_id
										WHERE
											execution_property_override_values.execution_id = @execution_id);	
										
					UPDATE ##executionList 
					SET ##executionList.Qtd_execution_property_override_values = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
										
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 							
														
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	execution_property_override_values.property_id
																FROM SSISDB.internal.execution_property_override_values									
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = execution_property_override_values.execution_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE execution_property_override_values
									FROM SSISDB.internal.execution_property_override_values
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.property_id = execution_property_override_values.property_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.execution_property_override_values.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;
								
								UPDATE ##executionList 
								SET ##executionList.Qtd_execution_property_override_values = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
								
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH		
			
				/*
					internal.executions
				*/
	
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(executions.execution_id) AS Qtd
										FROM SSISDB.internal.executions
											INNER JOIN ##executionList
												ON ##executionList.execution_id = executions.execution_id
										WHERE
											executions.execution_id = @execution_id);
	
					UPDATE ##executionList 
					SET ##executionList.Qtd_executions = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
										
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 										
	
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	executions.execution_id
																FROM SSISDB.internal.executions									
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = executions.execution_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE executions
									FROM SSISDB.internal.executions
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.execution_id = executions.execution_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.executions.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;
	
								UPDATE ##executionList 
								SET ##executionList.Qtd_executions = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
								
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH
			
			/***************************************************
				internal.operations and its dependencies
			***************************************************/
				
				/*
					internal.operation_messages
				*/
			
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(operation_messages.operation_message_id) AS Qtd
										FROM SSISDB.internal.operation_messages
											INNER JOIN ##executionList
												ON ##executionList.execution_id = operation_messages.operation_id
										WHERE
											##executionList.execution_id = @execution_id);		
										
					UPDATE ##executionList 
					SET ##executionList.Qtd_operation_messages = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
										
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 										
	
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	operation_messages.operation_message_id
																FROM SSISDB.internal.operation_messages									
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = operation_messages.operation_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE operation_messages
									FROM SSISDB.internal.operation_messages
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.operation_message_id = operation_messages.operation_message_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.operation_messages.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;	
	
								UPDATE ##executionList 
								SET ##executionList.Qtd_operation_messages = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
								
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH	
			
				/*
					internal.extended_operation_info
				*/
			
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(extended_operation_info.info_id) AS Qtd
										FROM SSISDB.internal.extended_operation_info
											INNER JOIN ##executionList
												ON ##executionList.execution_id = extended_operation_info.operation_id
										WHERE
											##executionList.execution_id = @execution_id);		
	
					UPDATE ##executionList 
					SET ##executionList.Qtd_extended_operation_info = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
										
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 								
	
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	extended_operation_info.info_id
																FROM SSISDB.internal.extended_operation_info									
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = extended_operation_info.operation_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE extended_operation_info
									FROM SSISDB.internal.extended_operation_info
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.info_id = extended_operation_info.info_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.extended_operation_info.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;	
								
								UPDATE ##executionList 
								SET ##executionList.Qtd_extended_operation_info = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
	
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH	
		
				/*
					internal.operation_os_sys_info
				*/
	
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(operation_os_sys_info.info_id) AS Qtd
										FROM SSISDB.internal.operation_os_sys_info
											INNER JOIN ##executionList
												ON ##executionList.execution_id = operation_os_sys_info.operation_id
										WHERE
											##executionList.execution_id = @execution_id);
					
					UPDATE ##executionList 
					SET ##executionList.Qtd_operation_os_sys_info = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
										
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 						
	
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	operation_os_sys_info.info_id
																FROM SSISDB.internal.operation_os_sys_info									
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = operation_os_sys_info.operation_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE operation_os_sys_info
									FROM SSISDB.internal.operation_os_sys_info
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.info_id = operation_os_sys_info.info_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.operation_os_sys_info.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;	
	
								UPDATE ##executionList 
								SET ##executionList.Qtd_operation_os_sys_info = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
								
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH	
			
				/*
					internal.validations
				*/
	
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(validations.validation_id) AS Qtd
										FROM SSISDB.internal.validations
											INNER JOIN ##executionList
												ON ##executionList.execution_id = validations.validation_id
										WHERE
											##executionList.execution_id = @execution_id);		
										
					UPDATE ##executionList 
					SET ##executionList.Qtd_validations = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
										
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 			
		
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	validations.validation_id
																FROM SSISDB.internal.validations									
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = validations.validation_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE validations
									FROM SSISDB.internal.validations
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.validation_id = validations.validation_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.validations.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;
	
								UPDATE ##executionList 
								SET ##executionList.Qtd_validations = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
								
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH	
			
				/*
					internal.operation_permissions
				*/
	
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(operation_permissions.id) AS Qtd
										FROM SSISDB.internal.operation_permissions
											INNER JOIN ##executionList
												ON ##executionList.execution_id = operation_permissions.[object_id]
										WHERE
											##executionList.execution_id = @execution_id);
										
					UPDATE ##executionList 
					SET ##executionList.Qtd_operation_permissions = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
										
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 		
	
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	operation_permissions.id
																FROM SSISDB.internal.operation_permissions									
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = operation_permissions.[object_id]
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE operation_permissions
									FROM SSISDB.internal.operation_permissions
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.id = operation_permissions.id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.operation_permissions.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;
								
								UPDATE ##executionList 
								SET ##executionList.Qtd_operation_permissions = @CounterRow
								WHERE 
									##executionList.execution_id = @execution_id;
								
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH	
			
				/*
					internal.operations
				*/
	
				BEGIN TRY
	
					SET @CounterRow = (SELECT					
											COUNT(operations.operation_id) AS Qtd
										FROM SSISDB.internal.operations
											INNER JOIN ##executionList
												ON ##executionList.execution_id = operations.operation_id
										WHERE
											##executionList.execution_id = @execution_id);	
										
					UPDATE ##executionList 
					SET ##executionList.Qtd_operations = @CounterRow 
					WHERE 
						##executionList.execution_id = @execution_id;
										
					BEGIN TRANSACTION
	
						WHILE (@CounterRow > 0)	
							BEGIN 		
	
								SET @SQLStatement = CONCAT(';WITH CTEDeleteRow AS (
																SELECT TOP (', @QtdDeletedRowToDeletePerCycle, ') 					
																	operations.operation_id
																FROM SSISDB.internal.operations									
																	INNER JOIN ##executionList
																		ON ##executionList.execution_id = operations.operation_id
																WHERE
																	##executionList.execution_id = ', @execution_id, ' ORDER BY 1)		
									
									DELETE operations
									FROM SSISDB.internal.operations
										INNER JOIN CTEDeleteRow										
											ON CTEDeleteRow.operation_id = operations.operation_id;');
	
								EXECUTE sp_executesql @SQLStatement;
								
								SET @QtdDeletedRow = @@ROWCOUNT;
								
								SET @msg = CONCAT('Deleted ', @QtdDeletedRow, ' of ', @CounterRow, ' rows from internal.operations.');						
								
								RAISERROR (@msg, 10, 1) WITH NOWAIT;
								
								SET @CounterRow = @CounterRow - @QtdDeletedRow;
	
								UPDATE ##executionList 
								SET ##executionList.Qtd_operations = @CounterRow 
								WHERE 
									##executionList.execution_id = @execution_id;
								
							END
	
					COMMIT TRANSACTION
	
				END TRY
	
				BEGIN CATCH
				
					INSERT INTO ErrorControlScript (Script, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
					SELECT
						@Script AS Script,
						ERROR_NUMBER() AS ErrorNumber,
						ERROR_SEVERITY() AS ErrorSeverity,
						ERROR_STATE() AS ErrorState,
						ERROR_PROCEDURE() AS ErrorProcedure,
						ERROR_LINE() AS ErrorLine,
						ERROR_MESSAGE() AS ErrorMessage
	        
					-- Test if the transaction is uncommittable.  
					IF (XACT_STATE()) = -1  
						BEGIN  
							PRINT N'The transaction is in an uncommittable state. Rolling back transaction.';  
							ROLLBACK TRANSACTION;  
						END;  
					
					-- Test if the transaction is committable.  
					IF (XACT_STATE()) = 1  
						BEGIN  
							PRINT N'The transaction is committable. Committing transaction.';  
							COMMIT TRANSACTION;     
						END;
					
				END CATCH	
	
			UPDATE ##executionList 
			SET isDeleted = 1, 
				endTime = GETDATE() 
			WHERE 
				##executionList.execution_id = @execution_id;
		
		END

		SET NOEXEC OFF
END