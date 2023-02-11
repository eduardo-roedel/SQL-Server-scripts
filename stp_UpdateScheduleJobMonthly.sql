/*Execute
DECLARE @JobName SYSNAME = N'';
DECLARE @ScheduleName_LastWeekOfMonth SYSNAME = N'DBA - Temporary Custom disabler - Last week%';
DECLARE @considerLastDayOfMonth BIT = 1;
DECLARE @ScheduleName_LastDayOfMonth SYSNAME = N'DBA - Temporary Custom disabler - Last day%';

EXECUTE dbo.stp_UpdateScheduleJobMonthly
@JobName = @JobName,
@ScheduleName_LastWeekOfMonth = @ScheduleName_LastWeekOfMonth,
@considerLastDayOfMonth = @considerLastDayOfMonth,
@ScheduleName_LastDayOfMonth = @ScheduleName_LastDayOfMonth;
*/

USE [master]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[stp_UpdateScheduleJobMonthly] (
	@JobName NVARCHAR(128),
	@ScheduleName_LastWeekOfMonth NVARCHAR(128),
	@considerLastDayOfMonth BIT,
	@ScheduleName_LastDayOfMonth NVARCHAR(128)
) 
AS 
SET NOCOUNT ON;

BEGIN

	DECLARE @Date DATE;	
	DECLARE @LastDayMonth DATE;
	DECLARE @schedule_id INT;
	DECLARE @job_id UNIQUEIDENTIFIER;
	DECLARE @active_start_date_INT INT;
	DECLARE @active_end_date_INT INT;
	DECLARE @active_start_time INT;
	DECLARE @active_end_time INT;
	DECLARE @freq_type INT;
	DECLARE @CurrentScheduleName SYSNAME;
	DECLARE @Message VARCHAR(MAX);
	DECLARE @new_active_start_date DATE;
	DECLARE @new_active_end_date DATE;

	DECLARE @TableDay AS TABLE (
		Id TINYINT IDENTITY (1,1) PRIMARY KEY,
		DateSchedule DATE,
		YearSchedule AS DATEPART(YEAR, DateSchedule),
		MonthSchedule AS DATEPART(MONTH, DateSchedule),
		DaySchedule AS DATEPART(DAY, DateSchedule),
		[DayNumber] AS DATEPART(WEEKDAY, DateSchedule),
		[DayOfWeek] AS DATENAME(WEEKDAY, DateSchedule),
		[WeekOfYear] AS DATENAME(WEEK, DateSchedule),
		WeekOfMonth TINYINT
	);
	
	DECLARE @TableJobSchedule AS TABLE (
		Id TINYINT IDENTITY(1,1) PRIMARY KEY,
		job_id UNIQUEIDENTIFIER,
		JobName SYSNAME,
		schedule_id INT,
		ScheduleName SYSNAME,
		active_start_date INT,
		active_end_date INT,		
		active_start_time INT,
		active_end_time INT,
		freq_type INT,
		new_active_start_date DATE,
		new_active_end_date DATE
	);

	SET @Date = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1);
	SET @LastDayMonth = DATEADD(DAY, -1, DATEADD(MONTH, 1, @Date));

	IF @considerLastDayOfMonth IS NULL
		BEGIN
			SET @considerLastDayOfMonth = 0;
		END
	
	IF @JobName IS NULL
		BEGIN
		  SET @Message = 'Fill the variable @JobName correctly.'
		  RAISERROR(@Message, 16, 1) WITH NOWAIT
		END

	IF NOT EXISTS (SELECT sysjobs.name FROM msdb.dbo.sysjobs WHERE sysjobs.name = @JobName)
		BEGIN
		  SET @Message = 'The job does not exist.'
		  RAISERROR(@Message, 16, 1) WITH NOWAIT
		END

	IF NOT EXISTS (SELECT sysschedules.[name] 
					FROM msdb.dbo.sysjobs
						INNER JOIN msdb.dbo.sysjobschedules
							ON sysjobschedules.job_id = sysjobs.job_id
						INNER JOIN msdb.dbo.sysschedules
							ON sysschedules.schedule_id = sysjobschedules.schedule_id
					WHERE
						sysjobs.name = @JobName
						AND sysschedules.[name] LIKE '' + @ScheduleName_LastWeekOfMonth + '%') 
		BEGIN
		  SET @Message = 'The job''s schedule @ScheduleName_LastWeekOfMonth does not exist.'
		  RAISERROR(@Message, 16, 1) WITH NOWAIT
		END	
	
	IF (@considerLastDayOfMonth = 1 AND @ScheduleName_LastDayOfMonth IS NULL) OR (@considerLastDayOfMonth = 0 AND @ScheduleName_LastDayOfMonth IS NOT NULL)
		BEGIN
		  SET @Message = 'If you want to use a different schedule for last day of month, you MUST have to use the variables @considerLastDayOfMonth AND @ScheduleName_LastDayOfMonth correctly.'
		  RAISERROR(@Message, 16, 1) WITH NOWAIT
		END

	IF NOT EXISTS (SELECT 
						sysschedules.[name] 
					FROM msdb.dbo.sysjobs
						INNER JOIN msdb.dbo.sysjobschedules
							ON sysjobschedules.job_id = sysjobs.job_id
						INNER JOIN msdb.dbo.sysschedules
							ON sysschedules.schedule_id = sysjobschedules.schedule_id
					WHERE
						sysjobs.name = @JobName
						AND sysschedules.[name] LIKE '' + @ScheduleName_LastDayOfMonth + '%') 
		BEGIN
		  SET @Message = 'The job''s schedule @ScheduleName_LastDayOfMonth does not exist.'
		  RAISERROR(@Message, 16, 1) WITH NOWAIT
		END	

	;WITH CTE AS (
		SELECT 
			@Date AS DateSchedule			
		UNION ALL
		SELECT
			DATEADD(DAY, 1, CTE.DateSchedule) AS DateSchedule
		FROM CTE
		WHERE
			CTE.DateSchedule <= @LastDayMonth
	)

	INSERT INTO @TableDay (DateSchedule, WeekOfMonth)			
	SELECT
		CTE.DateSchedule,
		DENSE_RANK() OVER (ORDER BY DATEPART(YEAR, DateSchedule), DATEPART(MONTH, DateSchedule), DATENAME(WEEK, DateSchedule) ASC) AS WeekOfMonth
	FROM CTE;

	;WITH ScheduleList AS (
		SELECT	
			sysjobs.job_id,
			sysjobs.name AS JobName,
			sysschedules.schedule_id,
			sysschedules.[name] AS ScheduleName,
			sysschedules.active_start_date,
			sysschedules.active_end_date,
			sysschedules.active_start_time,
			sysschedules.active_end_time,
			sysschedules.freq_type
		FROM msdb.dbo.sysjobs
			INNER JOIN msdb.dbo.sysjobschedules
				ON sysjobschedules.job_id = sysjobs.job_id
			INNER JOIN msdb.dbo.sysschedules
				ON sysschedules.schedule_id = sysjobschedules.schedule_id
		WHERE
			sysjobs.name = @JobName
	)
		
	INSERT INTO @TableJobSchedule (job_id, JobName, schedule_id, ScheduleName, active_start_date, active_end_date, active_start_time, active_end_time, freq_type, new_active_start_date, new_active_end_date)
	SELECT
		List.job_id,
		List.JobName,
		List.schedule_id,
		List.ScheduleName,
		List.active_start_date,
		List.active_end_date,
		List.active_start_time,
		List.active_end_time,
		List.freq_type,		
		CAST(List.new_active_start_date AS DATE) AS new_active_start_date,
		CAST(List.new_active_end_date AS DATE) AS new_active_end_date
	FROM (SELECT
				ScheduleList.job_id,
				ScheduleList.JobName,
				ScheduleList.schedule_id,
				ScheduleList.ScheduleName,
				ScheduleList.active_start_date,
				ScheduleList.active_end_date,
				ScheduleList.active_start_time,
				ScheduleList.active_end_time,
				ScheduleList.freq_type,
				ListNewSchedule.new_active_start_date,
				ListNewSchedule.new_active_end_date
			FROM ScheduleList
				CROSS JOIN (SELECT DISTINCT
								MIN(Scheduler.min_active_start_date_week) OVER () AS new_active_start_date,
								MAX(Scheduler.max_active_end_date_week) OVER () AS new_active_end_date
							FROM (SELECT DISTINCT
										[@TableDay].WeekOfMonth,										
										MIN([@TableDay].DateSchedule) OVER (PARTITION BY [@TableDay].WeekOfMonth) AS min_active_start_date_week,
										MAX([@TableDay].DateSchedule) OVER (PARTITION BY [@TableDay].WeekOfMonth) AS max_active_end_date_week,
										MAX([@TableDay].WeekOfMonth) OVER () AS LastWeekMonth								
									FROM @TableDay
									WHERE
										[@TableDay].DateSchedule < @LastDayMonth) AS Scheduler
							WHERE
								Scheduler.WeekOfMonth < Scheduler.LastWeekMonth) AS ListNewSchedule
			
			WHERE
				ScheduleList.JobName = @JobName
				AND ScheduleList.ScheduleName NOT LIKE @ScheduleName_LastDayOfMonth
				AND ScheduleList.ScheduleName NOT LIKE @ScheduleName_LastWeekOfMonth
			
			UNION
			
			SELECT
				ScheduleList.job_id,
				ScheduleList.JobName,
				ScheduleList.schedule_id,
				ScheduleList.ScheduleName,
				ScheduleList.active_start_date,
				ScheduleList.active_end_date,
				ScheduleList.active_start_time,
				ScheduleList.active_end_time,
				ScheduleList.freq_type,
				ListNewSchedule.new_active_start_date,
				ListNewSchedule.new_active_end_date
			FROM ScheduleList
				CROSS JOIN (SELECT DISTINCT
								MIN(Scheduler.min_active_start_date_week) OVER () AS new_active_start_date,
								MAX(Scheduler.max_active_end_date_week) OVER () AS new_active_end_date
							FROM (SELECT DISTINCT
										[@TableDay].WeekOfMonth,										
										MIN([@TableDay].DateSchedule) OVER (PARTITION BY [@TableDay].WeekOfMonth) AS min_active_start_date_week,
										MAX([@TableDay].DateSchedule) OVER (PARTITION BY [@TableDay].WeekOfMonth) AS max_active_end_date_week,
										MAX([@TableDay].WeekOfMonth) OVER () AS LastWeekMonth								
									FROM @TableDay
									WHERE
										[@TableDay].DateSchedule < @LastDayMonth) AS Scheduler
							WHERE
								Scheduler.WeekOfMonth = Scheduler.LastWeekMonth) AS ListNewSchedule
			
			WHERE
				ScheduleList.JobName = @JobName
				AND ScheduleList.ScheduleName LIKE @ScheduleName_LastWeekOfMonth

			UNION

			SELECT
				ScheduleList.job_id,
				ScheduleList.JobName,
				ScheduleList.schedule_id,
				ScheduleList.ScheduleName,
				ScheduleList.active_start_date,
				ScheduleList.active_end_date,
				ScheduleList.active_start_time,
				ScheduleList.active_end_time,
				ScheduleList.freq_type,
				ListNewSchedule.min_active_start_date,
				ListNewSchedule.max_active_end_date
			FROM ScheduleList
				CROSS JOIN (SELECT DISTINCT
								MIN(Scheduler.min_active_start_date_week) OVER () AS min_active_start_date,
								MAX(Scheduler.max_active_end_date_week) OVER () AS max_active_end_date
							FROM (SELECT DISTINCT
										[@TableDay].WeekOfMonth,										
										MIN([@TableDay].DateSchedule) OVER (PARTITION BY [@TableDay].WeekOfMonth) AS min_active_start_date_week,
										MAX([@TableDay].DateSchedule) OVER (PARTITION BY [@TableDay].WeekOfMonth) AS max_active_end_date_week,
										MAX([@TableDay].WeekOfMonth) OVER () AS LastWeekMonth								
									FROM @TableDay
									WHERE
										[@TableDay].DateSchedule > @LastDayMonth) AS Scheduler
							WHERE
								Scheduler.WeekOfMonth = Scheduler.LastWeekMonth) AS ListNewSchedule
			
			WHERE
				ScheduleList.JobName = @JobName
				AND ScheduleList.ScheduleName LIKE @ScheduleName_LastDayOfMonth) AS List
	
	
	WHILE (SELECT COUNT(*) FROM @TableJobSchedule) > 0
		BEGIN	
			SELECT TOP 1
				@schedule_id = [@TableJobSchedule].schedule_id,
				@job_id = [@TableJobSchedule].job_id,
				@CurrentScheduleName = [@TableJobSchedule].ScheduleName,
				@active_start_time = [@TableJobSchedule].active_start_time,
				@active_end_time = [@TableJobSchedule].active_end_time,
				@freq_type = [@TableJobSchedule].freq_type,
				@new_active_start_date = [@TableJobSchedule].new_active_start_date,
				@new_active_end_date = [@TableJobSchedule].new_active_end_date
			FROM @TableJobSchedule	
	
			IF @considerLastDayOfMonth = 1 AND @ScheduleName_LastDayOfMonth IS NOT NULL
				BEGIN						

					IF @CurrentScheduleName LIKE @ScheduleName_LastWeekOfMonth				
						BEGIN
							
							SET @active_start_date_INT = REPLACE(@new_active_start_date, '-','');
							SET @active_end_date_INT = REPLACE(@new_active_end_date, '-','');						
							
							EXECUTE msdb.dbo.sp_update_schedule   
								@schedule_id = @schedule_id,      		
								@active_start_date = @active_start_date_INT,
								@active_end_date = @active_end_date_INT,
								@enabled = 1,
								@freq_type = @freq_type,
								@active_start_time = @active_start_time,
								@active_end_time = @active_end_time;
								
						END

					IF @CurrentScheduleName LIKE @ScheduleName_LastDayOfMonth					
						BEGIN
						
							SET @active_start_date_INT = REPLACE(@new_active_start_date, '-','');
							SET @active_end_date_INT = 99991231;
							
							EXECUTE msdb.dbo.sp_update_schedule   
								@schedule_id = @schedule_id,      		
								@active_start_date = @active_start_date_INT,
								@active_end_date = @active_end_date_INT,
								@enabled = 1,
								@freq_type = @freq_type,
								@active_start_time = @active_start_time,
								@active_end_time = @active_end_time;
						END

					IF @CurrentScheduleName NOT IN (@ScheduleName_LastDayOfMonth, @ScheduleName_LastWeekOfMonth)
						BEGIN		
	
							SET @active_start_date_INT = REPLACE(@new_active_start_date, '-','');
							SET @active_end_date_INT = REPLACE(@new_active_end_date, '-','');				
	
							EXECUTE msdb.dbo.sp_update_schedule   
								@schedule_id = @schedule_id,      		
								@active_start_date = @active_start_date_INT,
								@active_end_date = @active_end_date_INT,
								@enabled = 1,
								@freq_type = @freq_type,
								@active_start_time = @active_start_time,
								@active_end_time = @active_end_time;
						END
				END
					ELSE
						BEGIN			
	
							SET @active_start_date_INT = REPLACE(@new_active_start_date, '-','');
							SET @active_end_date_INT = REPLACE(@new_active_end_date, '-','');					

							EXECUTE msdb.dbo.sp_update_schedule   
								@schedule_id = @schedule_id,      		
								@active_start_date = @active_start_date_INT,
								@active_end_date = @active_end_date_INT,
								@enabled = 1,
								@freq_type = @freq_type,
								@active_start_time = @active_start_time,
								@active_end_time = @active_end_time;
						END
	
			DELETE FROM @TableJobSchedule 
			WHERE 
				[@TableJobSchedule].schedule_id = @schedule_id
				AND [@TableJobSchedule].job_id = @job_id			
		END	
END
