--https://eduardoroedel.medium.com/dicas-para-manter-sua-msdb-em-ordem-parte-1-9936c241267f

DECLARE @StartDate DATETIME = (SELECT CAST(MIN(sysmaintplan_log.start_time) AS DATE) AS MinMaintPlanLogDate FROM msdb.dbo.sysmaintplan_log);
DECLARE @DayToRetain SMALLINT = 30;
DECLARE @oldest_date DATETIME = @StartDate;
DECLARE @Difference INT = DATEDIFF(DAY, @StartDate, DATEADD(DAY, - @DayToRetain, GETDATE()));


WHILE @Difference > 0
	BEGIN
		SET @oldest_date = DATEADD(DAY, 1, @oldest_date);
		
		EXECUTE msdb.dbo.sp_maintplan_delete_log 
		@plan_id = NULL,
		@subplan_id = NULL,
		@oldest_time = @oldest_date;			

		SET @Difference = @Difference - 1;		
	END
