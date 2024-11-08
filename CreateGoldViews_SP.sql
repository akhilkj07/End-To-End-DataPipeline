USE gold_db
GO

CREATE OR ALTER PROC CreateGoldViews @viewname NVARCHAR(100)
AS
BEGIN

DECLARE @statement VARCHAR(MAX)

    SET @statement = N'CREATE OR ALTER VIEW '+ @viewname + '_view AS
        SELECT * 
        FROM 
            OPENROWSET(
            BULK ''https://akjstorageaccproj1.dfs.core.windows.net/gold/SalesLT/'+@viewname+'/'',
            FORMAT = ''DELTA''
        ) as [result]
    '

EXEC (@statement)

END
GO
