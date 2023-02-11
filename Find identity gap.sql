USE [databasename];

DECLARE @StartDate DATETIME;
DECLARE @EndDate DATETIME;
DECLARE @Id INT;
DECLARE @TotalQtdRow INT;
DECLARE @QtdRow BIGINT; 
DECLARE @Ident_Current BIGINT;
DECLARE @Min_Id BIGINT;
DECLARE @Max_Id BIGINT;
DECLARE @StartId_FindInterval INT;
DECLARE @EndId_FindInterval INT;
DECLARE @TableSchema SYSNAME;
DECLARE @TableIdentityColumn SYSNAME;
DECLARE @TableDateColumn SYSNAME;
DECLARE @SQLStatement NVARCHAR(MAX);
DECLARE @sp_executesql NVARCHAR(MAX);

SET @TableSchema =  N'dbo.table';
SET @TableDateColumn = N'CreatedOn';
SET @TableIdentityColumn = N'Id';

IF OBJECT_ID('tempdb..##qtdList') IS NOT NULL
    BEGIN
        DROP TABLE ##qtdList;
    END	

CREATE TABLE ##qtdList (
    Id INT IDENTITY (1, 1) PRIMARY KEY,
    DateRow DATE,
    StartDate DATETIME,
    EndDate DATETIME,
    QtdRow BIGINT,
    State BIT DEFAULT 0 NOT NULL
);

SELECT
	@@SERVERNAME AS ServerName,
    DB_NAME() AS DatabaseName, 
    schemas.name AS SchemaName, 
    tables.name AS tableName, 
    partitions.rows AS RowCounts,
    CAST(ROUND((SUM(allocation_units.used_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS Used_MB, 
    CAST(ROUND((SUM(allocation_units.total_pages) - SUM(allocation_units.used_pages)) / 128.00, 2) AS NUMERIC(36, 2)) AS Unused_MB, 
    CAST(ROUND((SUM(allocation_units.total_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS Total_MB
FROM sys.tables
    INNER JOIN sys.indexes
       ON indexes.object_id = tables.object_id
    INNER JOIN sys.partitions
       ON partitions.object_id = indexes.object_id
    	  AND partitions.index_id = indexes.index_id
    INNER JOIN sys.allocation_units
       ON allocation_units.container_id = partitions.partition_id
    INNER JOIN sys.schemas
       ON schemas.schema_id = tables.schema_id
WHERE    
	tables.object_id = OBJECT_ID(@TableSchema)
GROUP BY
    tables.name, 
    schemas.name, 
    partitions.rows
ORDER BY
     partitions.rows DESC;

SET @sp_executesql = 'sys.sp_executesql';

SET @SQLStatement = N'SELECT @TotalQtdRow_Out = COUNT(*) FROM ' + @TableSchema + N';';
RAISERROR (@SQLStatement, 0, 1) WITH NOWAIT;

EXECUTE @sp_executesql
@stmt = @SQLStatement,
@params = N'@TotalQtdRow_Out AS BIGINT OUTPUT',
@TotalQtdRow_Out = @TotalQtdRow OUTPUT;

SET @SQLStatement = N'SELECT @Ident_Current_Out = IDENT_CURRENT(' + QUOTENAME(@TableSchema, '''') + N');';
RAISERROR (@SQLStatement, 0, 1) WITH NOWAIT;

EXECUTE @sp_executesql
@stmt = @SQLStatement,
@params = N'@Ident_Current_Out AS BIGINT OUTPUT',
@Ident_Current_Out = @Ident_Current OUTPUT;

SET @SQLStatement = N'SELECT @Min_Id_Out = MIN(' + @TableIdentityColumn + ') FROM ' + @TableSchema + N';';
RAISERROR (@SQLStatement, 0, 1) WITH NOWAIT;

EXECUTE @sp_executesql
@stmt = @SQLStatement,
@params = N'@Min_Id_Out AS BIGINT OUTPUT',
@Min_Id_Out = @Min_Id OUTPUT;

SET @SQLStatement = N'SELECT @Max_Id_Out = MAX(' + @TableIdentityColumn + ') FROM ' + @TableSchema + N';';
RAISERROR (@SQLStatement, 0, 1) WITH NOWAIT;

EXECUTE @sp_executesql
@stmt = @SQLStatement,
@params = N'@Max_Id_Out AS BIGINT OUTPUT',
@Max_Id_Out = @Max_Id OUTPUT;

SET @SQLStatement = N'SELECT @StartDate_Out = MIN(' + @TableDateColumn + ') FROM ' + @TableSchema + N';';
RAISERROR (@SQLStatement, 0, 1) WITH NOWAIT;

EXECUTE @sp_executesql
@stmt = @SQLStatement,
@params = N'@StartDate_Out AS DATETIME OUTPUT',
@StartDate_Out = @StartDate OUTPUT;

SET @SQLStatement = N'SELECT @EndDate_Out = MAX(' + @TableDateColumn + ') FROM ' + @TableSchema + N';';
RAISERROR (@SQLStatement, 0, 1) WITH NOWAIT;

EXECUTE @sp_executesql
@stmt = @SQLStatement,
@params = N'@EndDate_Out AS DATETIME OUTPUT',
@EndDate_Out = @EndDate OUTPUT;

SELECT
	@TotalQtdRow AS TotalQtdRow,
	@Ident_Current AS [Ident_Current],
	@Min_Id AS Min_Id,
	@Max_Id AS Max_Id,
	@StartDate AS StartDate,
	@EndDate AS EndDate,
	DATEDIFF(DAY, @StartDate, @EndDate) AS IntervalData;

SET @StartDate = FORMAT(@StartDate, 'yyyy-MM-dd 00:00:00.000');
SET @EndDate = DATEADD(MILLISECOND, -3, CAST(CAST(DATEADD(DAY, 1, GETDATE()) AS DATE) AS DATETIME)); 

;WITH ListDate AS (
    SELECT
        @StartDate AS StartDate,    
        FORMAT(@StartDate, 'yyyy-MM-dd 23:59:59.997') AS EndDate 

    UNION ALL 

    SELECT
        DATEADD(DAY, 1, ListDate.StartDate) AS StartDate,
        FORMAT(DATEADD(DAY, 1, ListDate.StartDate), 'yyyy-MM-dd 23:59:59.997') AS EndDate
    FROM ListDate
    WHERE
        ListDate.StartDate < @EndDate
)

INSERT INTO ##qtdList (DateRow, StartDate, EndDate)
SELECT
    FORMAT(ListDate.StartDate, 'yyyy-MM-dd') AS DateRow,   
    ListDate.StartDate,
    ListDate.EndDate
FROM ListDate
OPTION (MAXRECURSION 32767); 

SET @TotalQtdRow = (SELECT COUNT(*) FROM ##qtdList);

WHILE @TotalQtdRow > 0
    BEGIN

        SELECT TOP 1
            @Id = ##qtdList.Id,
            @StartDate = ##qtdList.StartDate,
            @EndDate = ##qtdList.EndDate
        FROM ##qtdList
        WHERE
            ##qtdList.State = 0; 

        SET @SQLStatement = N'SELECT @QtdRow_Out = COUNT(*) FROM ' + @TableSchema + N' WHERE ' + @TableSchema + N'.' + @TableDateColumn + N' BETWEEN @StartDate_In AND @EndDate_In;';
				
		EXECUTE @sp_executesql
		@stmt = @SQLStatement,
		@params = N'@StartDate_In AS DATETIME, @EndDate_In AS DATETIME, @QtdRow_Out AS BIGINT OUTPUT',
		@StartDate_In = @StartDate,
		@EndDate_In = @EndDate,
		@QtdRow_Out = @QtdRow OUTPUT;

        UPDATE ##qtdList SET ##qtdList.QtdRow = @QtdRow, ##QtdList.State = 1 WHERE ##qtdList.Id = @Id;

        SET @TotalQtdRow = @TotalQtdRow - 1;

    END 

SELECT * FROM ##qtdList;

SET @StartId_FindInterval = @Min_Id;
SET @EndId_FindInterval = @Ident_Current

SET @SQLStatement = N'SELECT
							*
						FROM (SELECT
									GapListIntermediate.Id,
									GapListIntermediate.NextId,
									GapListIntermediate.NextId - GapListIntermediate.Id AS Interval
								FROM (SELECT ' + @TableIdentityColumn + N' AS Id, LEAD(' + @TableIdentityColumn + N', 1, 0) OVER (ORDER BY ' + @TableIdentityColumn + N') AS NextId FROM ' + @TableSchema + N' WHERE ' + @TableIdentityColumn + N' BETWEEN @StartId_FindInterval_In AND @EndId_FindInterval_In) AS GapListIntermediate) AS GapList
						WHERE
							GapList.Interval <> 1
						ORDER BY 
							GapList.Id ASC';

EXECUTE @sp_executesql
@stmt = @SQLStatement,
@params = N'@StartId_FindInterval_In AS BIGINT, @EndId_FindInterval_In AS BIGINT',
@StartId_FindInterval_In = @StartId_FindInterval,
@EndId_FindInterval_In = @EndId_FindInterval;
