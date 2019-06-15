-----------------------------------------------------------------------------------------------------------------------
-- 050 - Administration.sql
-- Version 1.0.5
-- Look for the most recent version of this script at www.tf3604.com/temporal.
-- MIT License.  See the bottom of this file for details.
-----------------------------------------------------------------------------------------------------------------------

use AutoTracker;
go

-- sys.tables new columns
select object_id, name, temporal_type, temporal_type_desc, history_table_id
from sys.tables;

-- sys.columns new columns
select t.name, c.name, c.generated_always_type, c.generated_always_type_desc
from sys.tables t
inner join sys.schemas s on s.schema_id = t.schema_id
inner join sys.columns c on c.object_id = t.object_id
where s.name = 'dbo'
and t.name = 'Automobile';

-- sys.periods new catalog view
select p.name, p.period_type, p.period_type_desc, t.name, sc.name start_column_name, ec.name end_column_name
from sys.periods p
inner join sys.tables t on t.object_id = p.object_id
inner join sys.columns sc on sc.object_id = t.object_id and sc.column_id = p.start_column_id
inner join sys.columns ec on ec.object_id = t.object_id and ec.column_id = p.end_column_id;



-----------------------------------------------------------------------------------------------------------------------
-- Copyright 2017-2019, Brian Hansen (brian at tf3604.com).

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
