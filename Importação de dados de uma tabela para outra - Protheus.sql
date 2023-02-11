--Script 1
USE Protheus_R33;

/*
SET STATISTICS PROFILE ON;
 
CREATE INDEX CT2020W01 ON dbo.CT2020 (CT2_DATA, D_E_L_E_T_, CT2_FILIAL) WITH (DATA_COMPRESSION = PAGE);
 
SET STATISTICS PROFILE OFF;
*/

SET NOCOUNT ON;

DECLARE @sp_executesql NVARCHAR(MAX);
DECLARE @SQLStatement NVARCHAR(MAX);
DECLARE @QtdRowPerPage INT;
DECLARE @QtdRow BIGINT;
DECLARE @QtdPage INT;
DECLARE @Loop INT;
DECLARE @msg NVARCHAR(MAX);
DECLARE @RowCount BIGINT;
DECLARE @MaxRecno BIGINT;
DECLARE @TableSchemaSource SYSNAME;
DECLARE @TableSchemaSource_Filter NVARCHAR(MAX);
DECLARE @TableSchemaSource_Order NVARCHAR(MAX);
DECLARE @TableSchemaTarget SYSNAME;
DECLARE @CT2_FILIAL_New VARCHAR(2);

IF OBJECT_ID('tempdb..#RecnoList') IS NOT NULL
    BEGIN
        DROP TABLE #RecnoList;
    END

CREATE TABLE #RecnoList (
    [R_E_C_N_O_] BIGINT PRIMARY KEY,
    R_E_C_N_O_New BIGINT
);

SET @QtdRowPerPage = 50000;
SET @sp_executesql = N'sys.sp_executesql';

SET @TableSchemaSource = N'dbo.CT2020';
SET @TableSchemaSource_Filter = N'CT2_DATA BETWEEN ''20190101'' AND ''20220630'' AND D_E_L_E_T_ <> ''*'' AND CT2_FILIAL BETWEEN '' '' AND ''ZZ'' ';
SET @TableSchemaSource_Order = N' ORDER BY [R_E_C_N_O_] ASC ';

SET @TableSchemaTarget = N'dbo.CT2010';
SET @CT2_FILIAL_New = '02';

SET @SQLStatement = N'SELECT @QtdRow_Out = COUNT([R_E_C_N_O_]) FROM ' + @TableSchemaSource + N' WHERE ' + @TableSchemaSource_Filter + N';';

EXECUTE @sp_executesql
    @stmt = @SQLStatement,
    @params = N'@QtdRow_Out AS BIGINT OUTPUT',
    @QtdRow_Out = @QtdRow OUTPUT;

SET @QtdPage = CEILING((CAST(@QtdRow AS DECIMAL(20, 2)) / @QtdRowPerPage));
SET @Loop = @QtdPage;

SELECT
    @TableSchemaSource AS TableSchemaSource,
    @TableSchemaSource_Filter AS TableSchemaSource_Filter,
    @TableSchemaSource_Order AS TableSchemaSource_Order,
    @TableSchemaTarget AS TableSchemaTarget,
    @QtdRow AS QtdRowEstimated,
    @QtdRowPerPage AS QtdRowPerPage,
    @QtdPage AS QtdPage,
    @Loop AS QtdLoop;

ExecutionCode:

/*Get MaxRecno*/
SET @SQLStatement = N'SELECT @MaxRecno_Out = MAX([R_E_C_N_O_]) FROM ' + @TableSchemaTarget + N';';

EXECUTE @sp_executesql
    @stmt = @SQLStatement,
    @params = N'@MaxRecno_Out AS BIGINT OUTPUT',
    @MaxRecno_Out = @MaxRecno OUTPUT;
/*Get MaxRecno*/

SET @MaxRecno = CASE
                    WHEN @MaxRecno IS NULL THEN 1
                    WHEN @MaxRecno = 0 THEN 1
                        ELSE
                            @MaxRecno
                END;

IF @Loop > 0
    BEGIN

        SET @SQLStatement = N'SELECT [R_E_C_N_O_], (@MaxRecno_In + ROW_NUMBER() OVER (ORDER BY [R_E_C_N_O_] ASC)) AS [R_E_C_N_O_New] FROM ' + @TableSchemaSource + N' WHERE ' + @TableSchemaSource_Filter + N' ' + @TableSchemaSource_Order + N' OFFSET @QtdRowPerPage_In * (@QtdPage_In - 1) ROWS FETCH NEXT @QtdRowPerPage_In ROWS ONLY'
        SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + N' - Starting loop ' + CAST(@Loop AS NVARCHAR) + N'. Remaining: ' + CAST((@Loop - 1) AS NVARCHAR) + N'.';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;

        INSERT INTO #RecnoList WITH (TABLOCK) ([R_E_C_N_O_], R_E_C_N_O_New)
        EXECUTE @sp_executesql
            @stmt = @SQLStatement,
            @params = N'@MaxRecno_In AS BIGINT, @QtdRowPerPage_In AS INT, @QtdPage_In AS INT',
            @MaxRecno_In = @MaxRecno,
            @QtdRowPerPage_In = @QtdRowPerPage,
            @QtdPage_In = @QtdPage;

        SET @RowCount = @@ROWCOUNT;

        SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + N' - Preparing ' + CAST(@RowCount AS NVARCHAR) + N' rows to insert.';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
    END
        ELSE
            BEGIN
                SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + N' - Finished.';
                RAISERROR(@msg, 0, 1) WITH NOWAIT;
            END

WHILE @Loop > 0
    BEGIN

        SET @SQLStatement = (SELECT
                                N'INSERT INTO ' + @TableSchemaTarget + N' WITH (TABLOCK) ([R_E_C_N_O_], ' + 
                                STUFF((SELECT
                                            N',' + ColumnList.ColumnName 
                                        FROM (SELECT
                                                    QUOTENAME(columns.name) AS ColumnName
                                                FROM sys.columns
                                                WHERE
                                                    columns.object_id = OBJECT_ID(@TableSchemaTarget)
                                                    AND columns.name NOT LIKE 'R_E_C_N_O_%') AS ColumnList
                                        ORDER BY
                                            ColumnList.ColumnName ASC FOR XML PATH('')), 1, 1, '') + N') ' +
 
                            N'SELECT #RecnoList.[R_E_C_N_O_New] AS [R_E_C_N_O_], ' +
                                STUFF((SELECT
                                            /* Para o caso 1 */
                                            N',' + REPLACE(ColumnList.ColumnName, N'[CT2_FILIAL]', N'@CT2_FILIAL_New_In AS CT2_FILIAL')
                                            /* Para o caso 1 */
    
                                        FROM (SELECT
                                                    QUOTENAME(columns.name) AS ColumnName
                                                FROM sys.columns
                                                WHERE
                                                    columns.object_id = OBJECT_ID(@TableSchemaSource)
                                                    AND columns.name NOT LIKE 'R_E_C_N_O_%') AS ColumnList
                                        ORDER BY
                                            ColumnList.ColumnName ASC FOR XML PATH('')), 1, 1, '') + 
                            N' FROM ' + @TableSchemaSource + 
                            N' INNER JOIN #RecnoList ON #RecnoList.[R_E_C_N_O_] = ' + @TableSchemaSource + N'.[R_E_C_N_O_] OPTION (MAXDOP 4) ');

    BEGIN TRANSACTION

        EXECUTE @sp_executesql
            @stmt = @SQLStatement,
            @params = N'@CT2_FILIAL_New_In AS VARCHAR(2), @QtdRowPerPage_In AS INT, @QtdPage_In AS INT',
            @CT2_FILIAL_New_In = @CT2_FILIAL_New,
            @QtdRowPerPage_In = @QtdRowPerPage,
            @QtdPage_In = @QtdPage;

        SET @RowCount = @@ROWCOUNT;

    COMMIT TRANSACTION

    SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + N' - Finished loop ' + CAST(@Loop AS NVARCHAR) + N'. Inserted: ' + CAST(@RowCount AS NVARCHAR) + N' rows. Remaining: ' + CAST((@Loop - 1) AS NVARCHAR) + N'.';
    RAISERROR(@msg, 0, 1) WITH NOWAIT;

    TRUNCATE TABLE #RecnoList;

    SET @QtdPage += -1;
    SET @Loop += -1;

    GOTO ExecutionCode;
END


--Script 2
USE Protheus_R33;

SET NOCOUNT ON;

DECLARE @sp_executesql NVARCHAR(MAX);
DECLARE @SQLStatement NVARCHAR(MAX);
DECLARE @QtdRowPerPage INT;
DECLARE @QtdRow BIGINT;
DECLARE @QtdPage INT;
DECLARE @Loop INT;
DECLARE @msg NVARCHAR(MAX);
DECLARE @RowCount BIGINT;

DECLARE @MaxRecno BIGINT;

DECLARE @TableSchemaSource SYSNAME;
DECLARE @TableSchemaSource_Filter NVARCHAR(MAX);
DECLARE @TableSchemaSource_Order NVARCHAR(MAX);

DECLARE @TableSchemaTarget SYSNAME;

IF OBJECT_ID('tempdb..#RecnoList') IS NOT NULL
BEGIN
    DROP TABLE #RecnoList;
END

CREATE TABLE #RecnoList (
    [R_E_C_N_O_] BIGINT PRIMARY KEY,
    R_E_C_N_O_New BIGINT
);

SET @QtdRowPerPage = 50000;
SET @sp_executesql = N'sys.sp_executesql';

SET @TableSchemaSource = N'dbo.CT2120';
SET @TableSchemaSource_Filter = N'CT2_FILIAL BETWEEN '' '' AND ''ZZ'' ';
SET @TableSchemaSource_Order = N' ORDER BY [R_E_C_N_O_] ASC ';
SET @TableSchemaTarget = N'dbo.CT2030';
SET @SQLStatement = N'SELECT @QtdRow_Out = COUNT([R_E_C_N_O_]) FROM ' + @TableSchemaSource + N' WHERE ' + @TableSchemaSource_Filter + N';';

EXECUTE @sp_executesql
    @stmt = @SQLStatement,
    @params = N'@QtdRow_Out AS BIGINT OUTPUT',
    @QtdRow_Out = @QtdRow OUTPUT;

SET @QtdPage = CEILING((CAST(@QtdRow AS DECIMAL(20, 2)) / @QtdRowPerPage));
SET @Loop = @QtdPage;

SELECT
    @TableSchemaSource AS TableSchemaSource,
    @TableSchemaSource_Filter AS TableSchemaSource_Filter,
    @TableSchemaSource_Order AS TableSchemaSource_Order,
    @TableSchemaTarget AS TableSchemaTarget,
    @QtdRow AS QtdRowEstimated,
    @QtdRowPerPage AS QtdRowPerPage,
    @QtdPage AS QtdPage,
    @Loop AS QtdLoop;

ExecutionCode:

/*Get MaxRecno*/
SET @SQLStatement = N'SELECT @MaxRecno_Out = MAX([R_E_C_N_O_]) FROM ' + @TableSchemaTarget + N';';

EXECUTE @sp_executesql
    @stmt = @SQLStatement,
    @params = N'@MaxRecno_Out AS BIGINT OUTPUT',
    @MaxRecno_Out = @MaxRecno OUTPUT;
/*Get MaxRecno*/

SET @MaxRecno = CASE
                    WHEN @MaxRecno IS NULL THEN 1
                    WHEN @MaxRecno = 0 THEN 1
                        ELSE @MaxRecno
                END;

IF @Loop > 0
    BEGIN

        SET @SQLStatement = N'SELECT [R_E_C_N_O_], (@MaxRecno_In + ROW_NUMBER() OVER (ORDER BY [R_E_C_N_O_] ASC)) AS [R_E_C_N_O_New] FROM ' + @TableSchemaSource + N' WHERE ' + @TableSchemaSource_Filter + N' ' + @TableSchemaSource_Order + N' OFFSET @QtdRowPerPage_In * (@QtdPage_In - 1) ROWS FETCH NEXT @QtdRowPerPage_In ROWS ONLY'
        SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + N' - Starting loop ' + CAST(@Loop AS NVARCHAR) + N'. Remaining: ' + CAST((@Loop - 1) AS NVARCHAR) + N'.';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;

        INSERT INTO #RecnoList WITH (TABLOCK) ([R_E_C_N_O_], R_E_C_N_O_New)
        EXECUTE @sp_executesql
            @stmt = @SQLStatement,
            @params = N'@MaxRecno_In AS BIGINT, @QtdRowPerPage_In AS INT, @QtdPage_In AS INT',
            @MaxRecno_In = @MaxRecno,
            @QtdRowPerPage_In = @QtdRowPerPage,
            @QtdPage_In = @QtdPage;

        SET @RowCount = @@ROWCOUNT;

        SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + N' - Preparing ' + CAST(@RowCount AS NVARCHAR) + N' rows to insert.';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
    END
        ELSE
            BEGIN
                SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + N' - Finished.';
                RAISERROR(@msg, 0, 1) WITH NOWAIT;
            END

WHILE @Loop > 0
    BEGIN

        SET @SQLStatement = (SELECT
                                N'INSERT INTO ' + @TableSchemaTarget + N' WITH (TABLOCK) ([R_E_C_N_O_], ' +
                                STUFF((SELECT
                                            N',' + ColumnList.ColumnName
                                        FROM (SELECT
                                                    QUOTENAME(columns.name) AS ColumnName
                                                FROM sys.columns
                                                WHERE
                                                    columns.object_id = OBJECT_ID(@TableSchemaTarget)
                                                    AND columns.name NOT LIKE 'R_E_C_N_O_%') AS ColumnList
                                        ORDER BY
                                            ColumnList.ColumnName ASC FOR XML PATH('')), 1, 1, '') + N') ' + 
                                N'SELECT #RecnoList.[R_E_C_N_O_New] AS [R_E_C_N_O_], ' +
                                STUFF((SELECT
                                            /* Casos normais */
                                            N',' + ColumnList.ColumnName
                                            /* Casos normais */
                                        FROM (SELECT
                                                    QUOTENAME(columns.name) AS ColumnName
                                                FROM sys.columns
                                                WHERE
                                                    columns.object_id = OBJECT_ID(@TableSchemaSource)
                                                    AND columns.name NOT LIKE 'R_E_C_N_O_%') AS ColumnList
                                        ORDER BY
                                            ColumnList.ColumnName ASC FOR XML PATH('')), 1, 1, '') + N' FROM ' + @TableSchemaSource + N' INNER JOIN #RecnoList ON #RecnoList.[R_E_C_N_O_] = ' + @TableSchemaSource + N'.[R_E_C_N_O_] OPTION (MAXDOP 4) ');

        BEGIN TRANSACTION

            EXECUTE @sp_executesql
                @stmt = @SQLStatement,
                @params = N'@QtdRowPerPage_In AS INT, @QtdPage_In AS INT',
                @QtdRowPerPage_In = @QtdRowPerPage,
                @QtdPage_In = @QtdPage;

            SET @RowCount = @@ROWCOUNT;

        COMMIT TRANSACTION

        SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + N' - Finished loop ' + CAST(@Loop AS NVARCHAR) + N'. Inserted: ' + CAST(@RowCount AS NVARCHAR) + N' rows. Remaining: ' + CAST((@Loop - 1) AS NVARCHAR) + N'.';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;

        TRUNCATE TABLE #RecnoList;

        SET @QtdPage += -1;
        SET @Loop += -1;

        GOTO ExecutionCode;
    END