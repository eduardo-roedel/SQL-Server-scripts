SET NOCOUNT ON;

DECLARE @StartDate DATETIME;
DECLARE @EndDate DATETIME;
DECLARE @NOMETAB VARCHAR(32);
DECLARE @QtdTotalRow BIGINT;
DECLARE @RowCount INT;
DECLARE @QtdPerExecution INT;
DECLARE @DatetimeLimit DATETIME;
DECLARE @sp_executesql NVARCHAR(MAX);
DECLARE @SQLStatement NVARCHAR(MAX);
DECLARE @msg NVARCHAR(MAX);

DECLARE @TableList AS TABLE (
	NOMETAB VARCHAR(32) PRIMARY KEY,
	StartDate DATETIME,
	EndDate DATETIME
);

INSERT INTO @TableList (NOMETAB, StartDate, EndDate) VALUES
/* Manter os últimos 2 meses */
('TGFEST', NULL, DATEADD(MONTH, -2, GETDATE())),
('TGFCAB', NULL, DATEADD(MONTH, -2, GETDATE())),
('TGFFIN', NULL, DATEADD(MONTH, -2, GETDATE())),
('TGFITE', NULL, DATEADD(MONTH, -2, GETDATE())),
('TGFIXN', NULL, DATEADD(MONTH, -2, GETDATE())),
('TGFMBC', NULL, DATEADD(MONTH, -2, GETDATE())),
('TABLE1', NULL, DATEADD(MONTH, -2, GETDATE())),
/* Deletar os anos de 2018,2019,2020 */
('TABLE2', '2018-01-01 00:00:00.000', '2020-12-31 23:59:59.997'),
('TABLE3', '2018-01-01 00:00:00.000', '2020-12-31 23:59:59.997'),
('TABLE4', '2018-01-01 00:00:00.000', '2020-12-31 23:59:59.997'),
('TABLE5', '2018-01-01 00:00:00.000', '2020-12-31 23:59:59.997'),
('TABLE6', '2018-01-01 00:00:00.000', '2020-12-31 23:59:59.997'),
('TABLE7', '2018-01-01 00:00:00.000', '2020-12-31 23:59:59.997'),
('TGFEXC', '2018-01-01 00:00:00.000', '2020-12-31 23:59:59.997')
;

SET @QtdPerExecution = 5000;
SET @DatetimeLimit = FORMAT(GETDATE(), 'yyyy-MM-dd 06:00:00.000');
SET @sp_executesql = QUOTENAME(DB_NAME()) + N'.sys.sp_executesql';

WHILE (SELECT COUNT(*) FROM @TableList) > 0 AND GETDATE() < @DatetimeLimit
	BEGIN		
		
		SELECT TOP 1 
			@NOMETAB = [@TableList].NOMETAB,
			@StartDate = [@TableList].StartDate,
			@EndDate = [@TableList].EndDate
		FROM @TableList;		

		SET @msg = 'Selected ' + CAST(@NOMETAB AS NVARCHAR) + N' to purge, starting on ' + FORMAT(GETDATE(), 'dd/MM/yyyy HH:mm:ss:fff') + N'...';		
		RAISERROR (@msg, 0, 1) WITH NOWAIT;
		
		IF @StartDate IS NULL
			BEGIN
				SET @SQLStatement = N'SELECT @StartDate_Out = MIN(TSILGT.DHACAO) FROM sankhya.TSILGT WHERE TSILGT.NOMETAB = @NOMETAB_In AND TSILGT.DHACAO < @EndDate_In';	

				EXECUTE @sp_executesql 
					@stmt = @SQLStatement,
					@params = N'@StartDate_Out AS DATETIME OUTPUT, @NOMETAB_In AS VARCHAR(32), @EndDate_In AS DATETIME',
					@NOMETAB_In = @NOMETAB,
					@StartDate_Out = @StartDate OUTPUT,
					@EndDate_In = @EndDate;
			END

		SET @msg = '@StartDate from ' + CAST(@NOMETAB AS NVARCHAR) + N' to purge: ' + FORMAT(@StartDate, 'dd/MM/yyyy HH:mm:ss:fff') + N'...';		
		RAISERROR (@msg, 0, 1) WITH NOWAIT;
		
		SET @msg = '@EndDate from ' + CAST(@NOMETAB AS NVARCHAR) + N' to purge: ' + FORMAT(@EndDate, 'dd/MM/yyyy HH:mm:ss:fff') + N'...';		
		RAISERROR (@msg, 0, 1) WITH NOWAIT;

		SET @SQLStatement = N'SELECT @QtdTotalRow_Out = COUNT(*) FROM sankhya.TSILGT WHERE TSILGT.NOMETAB = @NOMETAB_In AND TSILGT.DHACAO BETWEEN @StartDate_In AND @EndDate_In;';
		
		EXECUTE @sp_executesql
			@stmt = @SQLStatement,
			@params = N'@QtdTotalRow_Out AS BIGINT OUTPUT, @NOMETAB_In AS VARCHAR(32), @StartDate_In AS DATETIME, @EndDate_In AS DATETIME',
			@QtdTotalRow_Out = @QtdTotalRow OUTPUT,		
			@NOMETAB_In = @NOMETAB,
			@StartDate_In = @StartDate,
			@EndDate_In = @EndDate;
		
		SET @msg = '@QtdTotalRow from ' + CAST(@NOMETAB AS NVARCHAR) + N' to purge: ' + CAST(@QtdTotalRow AS NVARCHAR) + N'...';				
		RAISERROR (@msg, 0, 1) WITH NOWAIT;
		
		SET @SQLStatement = N';WITH CTE_Delete AS (
		SELECT DISTINCT TOP (' + CAST(@QtdPerExecution AS NVARCHAR) + N') 
					TSILGT.NOMETAB, TSILGT.DHACAO
				FROM sankhya.TSILGT
				WHERE 
					TSILGT.NOMETAB = @NOMETAB_In
					AND TSILGT.DHACAO BETWEEN @StartDate_In AND @EndDate_In	
		)
		
		DELETE TSILGT
		FROM sankhya.TSILGT
			INNER JOIN CTE_Delete
				ON CTE_Delete.NOMETAB = TSILGT.NOMETAB
					AND CTE_Delete.DHACAO = TSILGT.DHACAO;';
		
		WHILE @QtdTotalRow > 0 AND GETDATE() < @DatetimeLimit
			BEGIN	
		
				EXECUTE @sp_executesql 
					@stmt = @SQLStatement,
					@params = N'@NOMETAB_In AS VARCHAR(32), @StartDate_In AS DATETIME, @EndDate_In AS DATETIME',
					@NOMETAB_In = @NOMETAB,
					@StartDate_In = @StartDate,
					@EndDate_In = @EndDate;
		
				SET @RowCount = @@ROWCOUNT;
		
				SET @QtdTotalRow = @QtdTotalRow - @RowCount;
		
				SET @msg = 'Deleted ' + CAST(@RowCount AS NVARCHAR) + N' rows. Remaining ' + CAST(@QtdTotalRow AS NVARCHAR) + N' rows. We''ll stop at ' + FORMAT(@DatetimeLimit, 'dd/MM/yyyy HH:mm:ss:fff') + N'. If this schedule isn''t enough, the next schedule will continue the activity...';		
				RAISERROR (@msg, 0, 1) WITH NOWAIT;
		
			END

		DELETE FROM @TableList WHERE [@TableList].NOMETAB = @NOMETAB;

		SET @msg = 'Finished table ' + @NOMETAB + N'. Selecting the next table. We''ll stop at ' + FORMAT(@DatetimeLimit, 'dd/MM/yyyy HH:mm:ss:fff') + N', if this schedule isn''t enough, the next schedule will continue the activity...';		
		RAISERROR (@msg, 0, 1) WITH NOWAIT;
END

SET @msg = 'It is ' + FORMAT(GETDATE(), 'dd/MM/yyyy HH:mm:ss:fff') + ' and we finished to purge all listed tables for today... :-)';		
RAISERROR (@msg, 0, 1) WITH NOWAIT;