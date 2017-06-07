use AutoTracker;
go

-- What happens when the history table gets bigger than we need?
-- Three methods to purge data out of history table:
-- 1: Custom script (this demo)
-- 2: Partitioning (next demo)
-- 3: Stretch DB (no demo)

-- Purging the history table via custom script.

-- Check our history and archive tables first:
select count(*) NbrRows from history.CustomerHistory;
select count(*) NbrRows from history.CustomerArchive;

-- Now try the purge

-- Let's just try what seems the most straightforward approach:
-- Identify about 100 rows in the CustomerHistory table.
begin transaction;

declare @cutoff datetime2;

select @cutoff = ValidTo
from history.CustomerHistory
order by ValidTo
offset 99 rows
fetch next 1 row only;

insert history.CustomerArchive (CustomerId, FirstName, LastName, Address, City, State, ValidFrom, ValidTo)
select CustomerId, FirstName, LastName, Address, City, State, ValidFrom, ValidTo
from history.CustomerHistory
where ValidTo <= @cutoff;

delete history.CustomerHistory
where ValidTo <= @cutoff;

commit transaction;
go

--Msg 13560, Level 16, State 1, Line 28
--Cannot delete rows from a temporal history table 'AutoTracker.history.CustomerHistory'.


-- OK, we'll disable system versioning, then do the purge.
-- But our workload is still running.  What if a transaction comes along and modified a Customer record
-- while the purge is running.  We don't miss any history just because we are purging.

-- We could just declare a maintenance window.   Or ...

-- Actually, we're OK.  So long as we disable and re-enable system versioning within a transaction,
-- activity against the table will block.  So long as the purge itself is small, the block should
-- be short enough that in most cases we don't need to worry about it.

begin transaction;

declare @cutoff datetime2;

select @cutoff = ValidTo
from history.CustomerHistory
order by ValidTo
offset 99 rows
fetch next 1 row only;

alter table dbo.Customer set (system_versioning = off);

insert history.CustomerArchive (CustomerId, FirstName, LastName, Address, City, State, ValidFrom, ValidTo)
select CustomerId, FirstName, LastName, Address, City, State, ValidFrom, ValidTo
from history.CustomerHistory
where ValidTo <= @cutoff;

delete history.CustomerHistory
where ValidTo <= @cutoff;

alter table dbo.Customer set (system_versioning = on (history_table = history.CustomerHistory, data_consistency_check = off));

commit transaction;
go

--Msg 13560, Level 16, State 1, Line 65
--Cannot delete rows from a temporal history table 'AutoTracker.history.CustomerHistory'.

-- Actually, the only problem is that SQL checks for delete from a history table at compile time.  Since history.CustomerHistory
-- is a valid history table at the time we compile this batch, we get the error.

-- The solution is to either
-- (1) Break up the above into multiple batches.  Kind of a pain because we need to deal with the variable somehow.
-- (2) Use dynamic SQL.  Also a pain.

declare @cutoff datetime2;

select @cutoff = ValidTo
from history.CustomerHistory
order by ValidTo
offset 99 rows
fetch next 1 row only;

declare @offSql nvarchar(max) = 'alter table dbo.Customer set (system_versioning = off);';

declare @insertSql nvarchar(max) = 'insert history.CustomerArchive (CustomerId, FirstName, LastName, Address, City, State, ValidFrom, ValidTo)
	select CustomerId, FirstName, LastName, Address, City, State, ValidFrom, ValidTo
	from history.CustomerHistory
	where ValidTo <= ''' + cast(@cutoff as nvarchar(30)) + ''';';

declare @deleteSql nvarchar(max) = 'delete history.CustomerHistory where ValidTo <= ''' + cast(@cutoff as nvarchar(30)) + ''';';

declare @onSql nvarchar(max) = 'alter table dbo.Customer set (system_versioning = on (history_table = history.CustomerHistory, data_consistency_check = off));';

begin transaction;
	exec (@offSql);
	exec (@insertSql);
	exec (@deleteSql);
	exec (@onSql);
commit transaction;
go

-- Check if it worked
select count(*) NbrRows from history.CustomerHistory;
select count(*) NbrRows from history.CustomerArchive;

select * from history.CustomerArchive;
