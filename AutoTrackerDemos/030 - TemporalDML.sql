-----------------------------------------------------------------------------------------------------------------------
-- 030 - TemporalDML.sql
-- Version 1.0.0
-- Look for the most recent version of this script at www.tf3604.com/temporal.
-- MIT License.  See the bottom of this file for details.
-----------------------------------------------------------------------------------------------------------------------
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



-----------------------------------------------------------------------------------------------------------------------
-- Copyright 2017, Brian Hansen (brian at tf3604.com).

-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
-- 
-- The above copyright notice and this permission notice shall be included in all
-- copies or substantial portions of the Software.
-- 
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
-- SOFTWARE.
-----------------------------------------------------------------------------------------------------------------------
