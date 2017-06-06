use AutoTracker;
go

-- sys.tables new columns
select name, temporal_type, temporal_type_desc, history_table_id
from sys.tables;

-- sys.columns new columns
select t.name, c.name, c.generated_always_type, c.generated_always_type_desc
from sys.tables t
inner join sys.schemas s on s.schema_id = t.schema_id
inner join sys.columns c on c.object_id = t.object_id
where s.name = 'dbo'
and t.name = 'Automobile';

-- sys.periods new catalog view
select p.name, p.period_type, p.period_type_desc, sc.name start_column_name, ec.name end_column_name
from sys.periods p
inner join sys.tables t on t.object_id = p.object_id
inner join sys.columns sc on sc.object_id = t.object_id and sc.column_id = p.start_column_id
inner join sys.columns ec on ec.object_id = t.object_id and ec.column_id = p.end_column_id;
