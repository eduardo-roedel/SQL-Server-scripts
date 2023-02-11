SET NOCOUNT ON;

DECLARE @ActualTime DATETIME;
DECLARE @StartTime DATETIME;
DECLARE @EndTime DATETIME;
DECLARE @TotalCount BIGINT;
DECLARE @RowCount BIGINT;
DECLARE @DaysForProdutoPreco INT;	 
DECLARE @TopRowExecution INT;
DECLARE @sqlStatement NVARCHAR(MAX);
DECLARE @sp_executesql NVARCHAR(MAX);
DECLARE @Message NVARCHAR(MAX);

SET @ActualTime = GETDATE();
SET @DaysForProdutoPreco = -180;
SET @TopRowExecution = 5000;   

IF OBJECT_ID('dbo.ProdutoPreco') IS NOT NULL
	BEGIN
		
		SET @EndTime = DATEADD(DAY, @DaysForProdutoPreco, @ActualTime);
		SET @StartTime = (SELECT MIN(DataCriacao) FROM dbo.ProdutoPreco WHERE ProdutoPreco.DataCriacao <= @EndTime);   	   	
		SET @TotalCount = (SELECT COUNT(*) FROM	dbo.ProdutoPreco WHERE ProdutoPreco.DataCriacao BETWEEN @StartTime AND @EndTime);
		
		SET @sp_executesql = QUOTENAME(DB_NAME()) + N'.[sys].[sp_executesql]';

		SET @sqlStatement = N';WITH CTE AS (
		 SELECT TOP ' + QUOTENAME(CAST(@TopRowExecution AS NVARCHAR), '()') + N' 
			ProdutoPreco.Codigo, 
			ProdutoPreco.TabelaPrecoCod, 
			ProdutoPreco.DataVigencia
		 FROM dbo.ProdutoPreco
		 WHERE
			ProdutoPreco.DataCriacao BETWEEN @StartTime_In AND @EndTime_In
		 ORDER BY
			ProdutoPreco.DataCriacao ASC		
		)
		
		DELETE ProdutoPreco
		FROM dbo.ProdutoPreco
			INNER JOIN CTE
				ON CTE.Codigo = ProdutoPreco.Codigo
					AND CTE.TabelaPrecoCod = ProdutoPreco.TabelaPrecoCod
					AND CTE.DataVigencia = ProdutoPreco.DataVigencia;';		
					

		SET @Message = N'Period to delete: ' + FORMAT(@StartTime, 'yyyy-MM-dd HH:mm:ss') + ' - ' + FORMAT(@EndTime, 'yyyy-MM-dd HH:mm:ss') + N' starting...' + CHAR(10);
		RAISERROR (@Message, 0, 1) WITH NOWAIT;

		WHILE @TotalCount > 0
			BEGIN
				
				SET @Message = N'Start: ' + FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' - Expected: ' + CAST(@TopRowExecution AS NVARCHAR) + ' row(s)';
				RAISERROR (@Message, 0, 1) WITH NOWAIT;

				EXECUTE @sp_executesql
					@stmt = @sqlStatement,
					@param = N'@StartTime_In AS DATETIME, @EndTime_In AS DATETIME',
					@StartTime_In = @StartTime,
					@EndTime_In = @EndTime;

				SET @RowCount = @@ROWCOUNT;
				SET @TotalCount = @TotalCount - @RowCount;

				SET @Message = @Message + N' - End: ' +	FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + N' - Deleted ' + CAST(@RowCount AS NVARCHAR) + ' row(s). Remaining: ' + CAST(@TotalCount AS NVARCHAR) + N' row(s).' + CHAR(10);
				RAISERROR (@Message, 0, 1) WITH NOWAIT;
			END										  
	END
SET NOCOUNT OFF;