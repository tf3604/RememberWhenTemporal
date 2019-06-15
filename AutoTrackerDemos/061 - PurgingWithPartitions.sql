-----------------------------------------------------------------------------------------------------------------------
-- 061 - PurgingWithPartitions.sql
-- Version 1.0.5
-- Look for the most recent version of this script at www.tf3604.com/temporal.
-- MIT License.  See the bottom of this file for details.
-----------------------------------------------------------------------------------------------------------------------

use AutoTracker;
go

----------------------------------------------------------------------------------------------------
-- Check counts on Ownership historical data.
----------------------------------------------------------------------------------------------------
select count(*) from history.OwnershipHistory;
select count(*) from history.OwnershipArchive;

-- Can see how many records are each partition.
-- Note that the first partition (the dummy partition) has no rows.
select p.partition_number, prv.value range_start_value, p.rows
from sys.partitions p
inner join sys.indexes i on p.object_id = i.object_id and i.index_id = p.index_id
inner join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
inner join sys.partition_functions pf on pf.function_id = ps.function_id
left join sys.partition_range_values prv on prv.function_id = pf.function_id and prv.boundary_id + 1 = p.partition_number
where p.object_id = object_id('history.OwnershipHistory')
and i.index_id = 1
order by p.partition_number;

-- And the partition in archive table are empty.
select p.partition_number, prv.value range_start_value, p.rows
from sys.partitions p
inner join sys.indexes i on p.object_id = i.object_id and i.index_id = p.index_id
inner join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
inner join sys.partition_functions pf on pf.function_id = ps.function_id
left join sys.partition_range_values prv on prv.function_id = pf.function_id and prv.boundary_id + 1 = p.partition_number
where p.object_id = object_id('history.OwnershipArchive')
and i.index_id = 1
order by p.partition_number;
go

----------------------------------------------------------------------------------------------------
-- Now we can do a little partitioning magic to move the partitions around.
----------------------------------------------------------------------------------------------------

-- Get the last partition range value.
declare @last_partition_value datetime;

select top 1 @last_partition_value = cast(prv.value as datetime)
from sys.partitions p
inner join sys.indexes i on p.object_id = i.object_id and i.index_id = p.index_id
inner join sys.data_spaces ds on ds.data_space_id = i.data_space_id
inner join sys.partition_schemes ps on ps.data_space_id = ds.data_space_id
inner join sys.partition_functions pf on pf.function_id = ps.function_id
left join sys.partition_range_values prv on prv.function_id = pf.function_id and prv.boundary_id + 1 = p.partition_number
where p.object_id = object_id('history.OwnershipHistory')
and i.name = 'idx_OwnershipHistory'
order by p.partition_number desc;

-- The next partition will be another 5 minutes in the future
declare @next_partition_value datetime = dateadd(minute, 5, @last_partition_value);

begin transaction;

alter table history.OwnershipHistory
switch partition 2 to history.OwnershipArchive partition 2;

-- Add a partition to OwnershipHistory for future inserts
alter partition scheme schemeOwnershipHistoryByEndTime
next used [primary];

alter partition function fnOwnershipHistoryPartitionByEndTime()
split range (@next_partition_value);

-- Add a partition to OwnershipArchive for future inserts
alter partition scheme schemeOwnershipArchiveByEndTime
next used [primary];

alter partition function fnOwnershipArchivePartitionByEndTime()
split range (@next_partition_value);

commit transaction;
go

----------------------------------------------------------------------------------------------------
-- Check the distribution of the data now.
----------------------------------------------------------------------------------------------------
select count(*) from history.OwnershipHistory;
select count(*) from history.OwnershipArchive;

-- Can see how many records are each partition.
-- Note that the first partition (the dummy partition) has no rows.
select p.partition_number, prv.value range_start_value, p.rows
from sys.partitions p
inner join sys.indexes i on p.object_id = i.object_id and i.index_id = p.index_id
inner join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
inner join sys.partition_functions pf on pf.function_id = ps.function_id
left join sys.partition_range_values prv on prv.function_id = pf.function_id and prv.boundary_id + 1 = p.partition_number
where p.object_id = object_id('history.OwnershipHistory')
and i.index_id = 1
order by p.partition_number;

-- And one of the partitions in archive table has rows.
select p.partition_number, prv.value range_start_value, p.rows
from sys.partitions p
inner join sys.indexes i on p.object_id = i.object_id and i.index_id = p.index_id
inner join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
inner join sys.partition_functions pf on pf.function_id = ps.function_id
left join sys.partition_range_values prv on prv.function_id = pf.function_id and prv.boundary_id + 1 = p.partition_number
where p.object_id = object_id('history.OwnershipArchive')
and i.index_id = 1
order by p.partition_number;
go

----------------------------------------------------------------------------------------------------
-- Next partition.
----------------------------------------------------------------------------------------------------

-- Get the last partition range value.
declare @last_partition_value datetime;

select top 1 @last_partition_value = cast(prv.value as datetime)
from sys.partitions p
inner join sys.indexes i on p.object_id = i.object_id and i.index_id = p.index_id
inner join sys.data_spaces ds on ds.data_space_id = i.data_space_id
inner join sys.partition_schemes ps on ps.data_space_id = ds.data_space_id
inner join sys.partition_functions pf on pf.function_id = ps.function_id
left join sys.partition_range_values prv on prv.function_id = pf.function_id and prv.boundary_id + 1 = p.partition_number
where p.object_id = object_id('history.OwnershipHistory')
and i.name = 'idx_OwnershipHistory'
order by p.partition_number desc;

-- The next partition will be another 5 minutes in the future
declare @next_partition_value datetime = dateadd(minute, 5, @last_partition_value);

begin transaction;

alter table history.OwnershipHistory
switch partition 3 to history.OwnershipArchive partition 3;

-- Add a partition to OwnershipHistory for future inserts
alter partition scheme schemeOwnershipHistoryByEndTime
next used [primary];

alter partition function fnOwnershipHistoryPartitionByEndTime()
split range (@next_partition_value);

-- Add a partition to OwnershipArchive for future inserts
alter partition scheme schemeOwnershipArchiveByEndTime
next used [primary];

alter partition function fnOwnershipArchivePartitionByEndTime()
split range (@next_partition_value);

commit transaction;
go

----------------------------------------------------------------------------------------------------
-- Check the distribution of the data now.
----------------------------------------------------------------------------------------------------
select count(*) from history.OwnershipHistory;
select count(*) from history.OwnershipArchive;

-- Can see how many records are each partition.
-- Note that the first partition (the dummy partition) has no rows.
select p.partition_number, prv.value range_start_value, p.rows
from sys.partitions p
inner join sys.indexes i on p.object_id = i.object_id and i.index_id = p.index_id
inner join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
inner join sys.partition_functions pf on pf.function_id = ps.function_id
left join sys.partition_range_values prv on prv.function_id = pf.function_id and prv.boundary_id + 1 = p.partition_number
where p.object_id = object_id('history.OwnershipHistory')
and i.index_id = 1
order by p.partition_number;

-- And the next partition in archive table now has rows.
select p.partition_number, prv.value range_start_value, p.rows
from sys.partitions p
inner join sys.indexes i on p.object_id = i.object_id and i.index_id = p.index_id
inner join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
inner join sys.partition_functions pf on pf.function_id = ps.function_id
left join sys.partition_range_values prv on prv.function_id = pf.function_id and prv.boundary_id + 1 = p.partition_number
where p.object_id = object_id('history.OwnershipArchive')
and i.index_id = 1
order by p.partition_number;
go



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
