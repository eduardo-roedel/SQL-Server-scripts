DECLARE @data1 DATETIME = '2021-06-12 19:00:01.000';
DECLARE @data2 DATETIME = '2021-06-12 20:00:00.000';
--03h 59m 59s

SELECT
	CONCAT( RIGHT(CONCAT('0', ((QueryResult.ColumnName / 60) / 60)) , 2) , ':', RIGHT(CONCAT('0', (QueryResult.ColumnName % 60)), 2) , ':',	RIGHT(CONCAT('0', ((QueryResult.ColumnName / 60) % 60)), 2) ) AS HoraVarchar	
FROM (SELECT 
		TableName.ColumnName
	  FROM (VALUES (DATEDIFF(SECOND, @data1, @data2))) AS TableName(ColumnName) ) AS QueryResult