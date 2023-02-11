DECLARE @DatabaseList AS TABLE (
	DatabaseName SYSNAME PRIMARY KEY
);

DECLARE @DatabaseName SYSNAME;
DECLARE @Statement NVARCHAR(MAX);
DECLARE @sp_executesql NVARCHAR(MAX);

INSERT INTO @DatabaseList (DatabaseName)
SELECT 
	databases.name 
FROM sys.databases 
WHERE 
	databases.database_id > 4
	AND databases.name <> 'SSISDB'   
	AND databases.name <> 'distribution';

DECLARE @TableList AS TABLE (
	Id INT IDENTITY (1, 1) PRIMARY KEY,
	InstanceName SYSNAME,
	DatabaseName SYSNAME,
	SchemaName SYSNAME,	
	TableName SYSNAME,
	HasPrimaryKey BIT,
	HasIdentity BIT,
	HasClusteredIndex BIT,
	HasUniqueConstraint BIT,
	Total_MB NUMERIC(36,2),
	RowCounts BIGINT
);

SET @Statement = N'SELECT
			@@SERVERNAME AS InstanceName,
			DB_NAME() AS DatabaseName,
			schemas.name AS SchemaName,
			objects.name AS TableName,
			OBJECTPROPERTY(objects.object_id, ''TableHasPrimaryKey'') AS HasPrimaryKey,	
			OBJECTPROPERTY(objects.object_id, ''TableHasIdentity'') AS HasIdentity,
			OBJECTPROPERTY(objects.object_id, ''TableHasClustIndex'') AS HasClusteredIndex,
			OBJECTPROPERTY(objects.object_id, ''TableHasUniqueCnst'') AS HasUniqueConstraint,
			SizeList.Total_MB,
			RowList.RowCounts	
		FROM sys.objects
			INNER JOIN sys.schemas
				ON schemas.schema_id = objects.schema_id
		    INNER JOIN sys.indexes
		        ON indexes.object_id = objects.object_id
			CROSS APPLY (SELECT	TOP 1				
							CAST(ROUND((SUM(allocation_units.total_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS Total_MB					
						FROM sys.partitions		
							INNER JOIN sys.allocation_units
							   ON allocation_units.container_id = partitions.partition_id
						WHERE
							partitions.object_id = indexes.object_id    				
						GROUP BY
							partitions.object_id) AS SizeList
			CROSS APPLY (SELECT	TOP 1				
							partitions.rows AS RowCounts
						FROM sys.partitions		
							INNER JOIN sys.allocation_units
							   ON allocation_units.container_id = partitions.partition_id
						WHERE
							partitions.object_id = indexes.object_id
		    				AND partitions.index_id = indexes.index_id) AS RowList
		    
		WHERE 
			objects.type = ''U''
			AND indexes.type = 0;';
			

	RAISERROR (@Statement, 0, 1);

WHILE (SELECT COUNT(*) FROM @DatabaseList) > 0
	BEGIN

		SET @DatabaseName = (SELECT TOP 1 [@DatabaseList].DatabaseName FROM @DatabaseList);

		SET @sp_executesql = QUOTENAME(@DatabaseName) + N'.sys.sp_executesql';
		RAISERROR (@sp_executesql, 0, 1);

		INSERT INTO @TableList (InstanceName, DatabaseName, SchemaName, TableName, HasPrimaryKey, HasIdentity, HasClusteredIndex, HasUniqueConstraint, Total_MB, RowCounts)
		EXECUTE @sp_executesql
		@stmt = @Statement;

		DELETE FROM @DatabaseList 
		WHERE 
			[@DatabaseList].DatabaseName = @DatabaseName;

	END

SELECT
	InstanceName,	
	DatabaseName,
	SchemaName,
	TableName,
	HasPrimaryKey,
	HasIdentity,
	HasClusteredIndex,
	HasUniqueConstraint,
	REPLACE(Total_MB, '.', ',') AS Total_MB,
	REPLACE(CAST(ROUND((Total_MB / 1024.0), 2) AS NUMERIC(36, 2)), '.', ',') AS Total_GB,
	RowCounts,
	CONCAT('ALTER TABLE ', QUOTENAME([@TableList].DatabaseName), '.', QUOTENAME([@TableList].SchemaName), '.', QUOTENAME([@TableList].TableName), ' ADD CONSTRAINT ', QUOTENAME(('PK_' + [@TableList].TableName)), ' PRIMARY KEY CLUSTERED (', ' ' ,') WITH (DATA_COMPRESSION = PAGE);') AS CreateConstraint
FROM @TableList 