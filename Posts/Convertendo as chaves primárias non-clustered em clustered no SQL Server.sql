--https://eduardoroedel.medium.com/convertendo-as-chaves-prim%C3%A1rias-non-clustered-em-clustered-no-sql-server-1252dec4d20a

--Drop and recreate foreign keys
SELECT	  
	QUOTENAME(foreign_keys.name) AS ForeignKey,
	QUOTENAME(schemasParent.name) AS schemasParentName,
	QUOTENAME(objectsParent.name) AS objectsParentName,
	columnsParent.columnsParentList, 
	QUOTENAME(schemasReference.name) AS schemasReferenceName,
	QUOTENAME(objectsReference.name) AS objectsReferenceName,
	columnsReference.columnsReferenceList,
	CONCAT('ALTER TABLE ', QUOTENAME(schemasParent.name), '.', QUOTENAME(objectsParent.name), ' DROP CONSTRAINT ', QUOTENAME(foreign_keys.name), ';') AS DroparFK,
	CONCAT('ALTER TABLE ', QUOTENAME(schemasParent.name), '.', QUOTENAME(objectsParent.name), ' WITH CHECK ADD CONSTRAINT ', QUOTENAME(foreign_keys.name), ' FOREIGN KEY (', columnsParent.columnsParentList, ') REFERENCES ' , QUOTENAME(schemasReference.name), '.', QUOTENAME(objectsReference.name), ' (', columnsReference.columnsReferenceList, ');') AS CriarFK
FROM sys.foreign_keys
	INNER JOIN sys.objects AS objectsParent
		ON objectsParent.object_id = foreign_keys.parent_object_id
	INNER JOIN sys.schemas AS schemasParent
		ON schemasParent.schema_id = objectsParent.schema_id
	
	CROSS APPLY (SELECT
					STUFF((SELECT 
						', ' , ISNULL(QUOTENAME(columns.[name]), '')
							FROM sys.foreign_key_columns
								INNER JOIN sys.columns
									ON columns.[object_id] = foreign_key_columns.parent_object_id
										AND columns.column_id = foreign_key_columns.parent_column_id
							WHERE 
								foreign_key_columns.constraint_object_id = foreign_keys.object_id							
							ORDER BY 
								foreign_key_columns.constraint_column_id FOR XML PATH('')), 1, 2, '') AS columnsParentList) AS columnsParent
	
	INNER JOIN sys.objects AS objectsReference
		ON objectsReference.object_id = foreign_keys.referenced_object_id
	INNER JOIN sys.schemas AS schemasReference
		ON schemasReference.schema_id = objectsReference.schema_id

	CROSS APPLY (SELECT
					STUFF((SELECT 
						', ' , ISNULL(QUOTENAME(columns.[name]), '')
							FROM sys.foreign_key_columns
								INNER JOIN sys.columns
									ON columns.[object_id] = foreign_key_columns.referenced_object_id
										AND columns.column_id = foreign_key_columns.referenced_column_id
							WHERE 
								foreign_key_columns.constraint_object_id = foreign_keys.object_id							
							ORDER BY 
								foreign_key_columns.constraint_column_id FOR XML PATH('')), 1, 2, '') AS columnsReferenceList) AS columnsReference
WHERE
	objectsParent.type = 'U'
	AND objectsReference.type = 'U'

GO

--Drop and recreate primary key clustered
SELECT	
	schemas.name AS SchemaName,
	objects.name AS ObjectName,	
	CONCAT('ALTER TABLE ', QUOTENAME(schemas.name), '.', QUOTENAME(objects.name), ' DROP CONSTRAINT ', QUOTENAME(indexes.name), ';') AS DropConstraint,
	CONCAT('ALTER TABLE ', QUOTENAME(schemas.name), '.', QUOTENAME(objects.name), ' ADD CONSTRAINT ', QUOTENAME(indexes.name), ' PRIMARY KEY CLUSTERED (', ColunaConstraint.ColumnList ,') WITH (DATA_COMPRESSION = PAGE);') AS CreateConstraint,
	ColunaConstraint.ColumnList
FROM sys.objects
	INNER JOIN sys.schemas
		ON schemas.schema_id = objects.schema_id
	INNER JOIN sys.indexes
		ON indexes.object_id = objects.object_id

	CROSS APPLY (SELECT
					STUFF((SELECT 
								', ' , ISNULL(QUOTENAME(columns.[name]), '')            
							FROM sys.index_columns
								INNER JOIN sys.columns
									ON columns.object_id = index_columns.object_id
										AND columns.column_id = index_columns.column_id
							WHERE 
								columns.[object_id] = indexes.[object_id]
								AND index_columns.index_id = indexes.index_id
							ORDER BY 
								index_columns.index_column_id FOR XML PATH('')), 1, 2, '') AS ColumnList) AS ColunaConstraint
WHERE
	objects.type = 'U'					
	AND objects.is_ms_shipped = 0
	AND indexes.is_primary_key = 1
	AND indexes.type = 2;