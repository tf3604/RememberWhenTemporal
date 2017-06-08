use AutoTracker2;
go

-- We defined the time columns in dbo.AutoModel as "hidden." 
-- They won't show up in "select *"
select * from dbo.AutoModel;

-- However, they can still be expressly queried.
select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
from dbo.AutoModel;

-- The time columns are also hidden from insert without express column list.
insert dbo.AutoModel
values (1, 'Brutus', 'Semi truck');

-- Note that in other tables, inserts will fail if we try to do this.
insert dbo.Manufacturer
values ('Knock-off, Inc');
--Msg 213, Level 16, State 1, Line 17
--Column name or number of supplied values does not match table definition.

-- We MUST specify the column list.  This is a good practice anyway.
insert dbo.Manufacturer (ManufacturerName)
values ('Knock-off, Inc');

-- Take a look at the tables we inserted into.
select * from dbo.Manufacturer;
select * from dbo.AutoModel;

-- What are those time columns?  Those times look to be in the future!
select sysutcdatetime() CurrentTime;

-- Let's check the history tables.  Inserts don't go in there.
-- Remember that SQL generated the history table for dbo.Manufacturer
select	t.object_id,
		s.name source_table_schema,
		t.name source_table_name,
		t.temporal_type_desc,
		hs.name history_table_schema,
		ht.name history_table_name
from sys.tables t
inner join sys.schemas s on t.schema_id = s.schema_id
inner join sys.tables ht on t.history_table_id = ht.object_id
inner join sys.schemas hs on ht.schema_id = hs.schema_id
where t.name = 'Manufacturer'
and s.name = 'dbo';

select * from dbo.MSSQL_TemporalHistoryFor_565577053;
select * from history.AutoModelHistory;

-- Cleanup
delete dbo.Manufacturer
where ManufacturerName = 'Knock-off, Inc';

delete dbo.AutoModel
where ModelName = 'Brutus';

-- This is a good time to see what happened to the deleted rows.
-- They are, of course, gone from the core tables.
select * from dbo.Manufacturer;
select * from dbo.AutoModel;

-- Check the history tables.
select * from dbo.MSSQL_TemporalHistoryFor_565577053;
select * from history.AutoModelHistory;

-- Let's update a row.
update dbo.AutoModel
set ModelName = 'TheCrusher'
where ModelId = 1;

-- Look at the core table and the history table.
-- Note that the ValidTo column in the history table equals the ValidFrom column in the core table.
-- The end time (ValidTo) should be interpreted as "less than."
-- The start time (ValidFrom) should be interpreted as "greater than or equal to."
select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo from history.AutoModelHistory where ModelId = 1;
select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo from dbo.AutoModel where ModelId = 1;

-- Now, let's re-write history.
update dbo.AutoModel
set ValidFrom = '2017-01-01'
where ModelId = 1;
--Msg 13537, Level 16, State 1, Line 80
--Cannot update GENERATED ALWAYS columns in table 'AutoTracker2.dbo.AutoModel'.

-- Similar for the history table.
update history.AutoModelHistory
set ValidFrom = '2017-01-01'
where ModelId = 1;
--Msg 13561, Level 16, State 1, Line 87
--Cannot update rows in a temporal history table 'AutoTracker2.history.AutoModelHistory'.