SET NOCOUNT ON;

DECLARE @StatisticList TABLE (
	Id INT IDENTITY(1,1) PRIMARY KEY,
	DatabaseId INT,
	DatabaseName SYSNAME,
	SchemaName SYSNAME,
	TableName SYSNAME,	
	StatisticOverlapping NVARCHAR(256),
	DropStatement AS (CONCAT('USE ', QUOTENAME(DatabaseName COLLATE DATABASE_DEFAULT), '; DROP STATISTICS ', QUOTENAME(SchemaName COLLATE DATABASE_DEFAULT), '.', QUOTENAME(TableName COLLATE DATABASE_DEFAULT), '.', QUOTENAME(StatisticOverlapping COLLATE DATABASE_DEFAULT), ';', CHAR(10))),
	WasPassedForward BIT DEFAULT (0)
);

DECLARE @DatabaseList AS TABLE (
	DatabaseName SYSNAME PRIMARY KEY,
	isDone BIT DEFAULT(0)
);

DECLARE @SQLStatement NVARCHAR(4000); 
DECLARE @DatabaseName SYSNAME;
DECLARE @StatisticAmount INT;
DECLARE @StatisticId INT;
DECLARE @msg NVARCHAR(MAX);

INSERT INTO @DatabaseList (DatabaseName)
SELECT
	databases.name
FROM sys.databases
WHERE
	databases.database_id > 4
	AND databases.source_database_id IS NULL
	AND databases.is_read_only = 0
	AND databases.state_desc = 'ONLINE';

SET @msg = CONCAT(@@ROWCOUNT, ' database(s) were selected', CHAR(10));
RAISERROR (@msg, 0, 1) WITH NOWAIT;

WHILE (SELECT COUNT(*) FROM @DatabaseList WHERE [@DatabaseList].isDone = 0) > 0
	BEGIN
		SET @DatabaseName = (SELECT TOP 1
								[@DatabaseList].DatabaseName 
							FROM @DatabaseList 
							WHERE 
								[@DatabaseList].isDone = 0);
		
		SET @msg = CONCAT('Database: ', @DatabaseName, ' was selected;', CHAR(10));
		RAISERROR (@msg, 0, 1) WITH NOWAIT;		

		SET @SQLStatement = N'USE ' + QUOTENAME(@DatabaseName) + '; ' + CHAR(10) + ' ;WITH autostats (databaseId, object_id, stats_id, name, column_id ) AS (   
	SELECT				   
		DB_ID() AS databaseId,
		stats.object_id,
		stats.stats_id,
		stats.name,
		stats_columns.column_id
	FROM sys.stats
	    INNER JOIN sys.stats_columns
	        ON stats_columns.object_id = stats.object_id
	           AND stats_columns.stats_id = stats.stats_id
	WHERE
	    stats.auto_created = 1
	    AND stats_columns.stats_column_id = 1
)

SELECT DISTINCT
	autostats.databaseId AS DatabaseId,
	DB_NAME(autostats.databaseId) AS DatabaseName,
	OBJECT_SCHEMA_NAME(stats.object_id) AS SchemaName,
	OBJECT_NAME(stats.object_id) AS TableName,
    /*columns.name AS ColumnName,   	
	stats.name AS [Overlapped],*/
    autostats.name AS [Overlapping]
FROM sys.stats
    INNER JOIN sys.stats_columns
        ON stats_columns.object_id = stats.object_id
           AND stats_columns.stats_id = stats.stats_id
    INNER JOIN autostats
        ON autostats.object_id = stats_columns.object_id
           AND autostats.column_id = stats_columns.column_id
    INNER JOIN sys.columns
        ON columns.object_id = stats.object_id
           AND columns.column_id = stats_columns.column_id
WHERE
    stats.auto_created = 0
    AND stats_columns.stats_column_id = 1
    AND stats_columns.stats_id <> autostats.stats_id
    AND OBJECTPROPERTY(stats.object_id, ''IsMsShipped'') = 0';

		SET @msg = CONCAT(@DatabaseName, '''s statistics insert started;');
		RAISERROR (@msg, 0, 1) WITH NOWAIT;		

		INSERT INTO @StatisticList (DatabaseId, DatabaseName, SchemaName, TableName, StatisticOverlapping)
		EXECUTE sp_executesql @SQLStatement;

		SET @msg = CONCAT(@@ROWCOUNT, ' statistic(s) from ', @DatabaseName, ' were inserted;', CHAR(10));
		RAISERROR (@msg, 0, 1) WITH NOWAIT;		

		UPDATE @DatabaseList SET [@DatabaseList].isDone = 1 WHERE [@DatabaseList].DatabaseName = @DatabaseName;		

	END	

	DELETE FROM @DatabaseList;	

	WHILE (SELECT COUNT(*) FROM @StatisticList) > 0
		BEGIN
			UPDATE @StatisticList SET [@StatisticList].WasPassedForward = 0 WHERE [@StatisticList].WasPassedForward = 1;	

				WHILE (SELECT COUNT(*) FROM @StatisticList WHERE WasPassedForward = 0) > 0
					BEGIN															
						
						SET @StatisticAmount = (SELECT 
													COUNT(*) 
												FROM @StatisticList
												WHERE
													[@StatisticList].WasPassedForward = 0);

						WHILE @StatisticAmount > 0
							BEGIN			
							
								SELECT TOP 1 
									@StatisticId = [@StatisticList].Id,
									@SQLStatement = [@StatisticList].DropStatement 
								FROM @StatisticList
								WHERE
									[@StatisticList].WasPassedForward = 0;								
								
								SET @msg = CONCAT(@StatisticId, ' - ', @SQLStatement, ' was selected;', CHAR(10));
								RAISERROR (@msg, 0, 1) WITH NOWAIT;								

								BEGIN TRY
									
									SET LOCK_TIMEOUT 50;
									
									EXECUTE sp_executesql @SQLStatement;

									DELETE FROM @StatisticList WHERE [@StatisticList].Id = @StatisticId;

									IF @@ROWCOUNT > 0
										BEGIN					
											SET @StatisticAmount = @StatisticAmount - @@ROWCOUNT;

											SET @msg = CONCAT(@StatisticId, ' - ', @SQLStatement, ' was dropped;', CHAR(10));
											RAISERROR (@msg, 0, 1) WITH NOWAIT;		
								
										END						

								END TRY

								BEGIN CATCH										

									SET @msg = CONCAT(@StatisticId, ' - ', @SQLStatement, ' wasn''t dropped yet, let''s try again after;', CHAR(10));
									RAISERROR (@msg, 0, 1) WITH NOWAIT;									
								
								END CATCH										

								UPDATE @StatisticList SET [@StatisticList].WasPassedForward = 1 
								WHERE [@StatisticList].Id = @StatisticId;				

								BREAK;

							END
					END
		END
SET NOCOUNT OFF;	


