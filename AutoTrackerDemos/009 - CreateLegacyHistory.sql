----------------------------------------------------------------------------------------------------
-- Create database.  Adjust paths as needed.
----------------------------------------------------------------------------------------------------
if exists (select * from sys.databases where name = 'LegacyAutoTracker')
begin
	alter database LegacyAutoTracker set offline with rollback immediate;
	alter database LegacyAutoTracker set online with rollback immediate;
	drop database LegacyAutoTracker;
end

create database LegacyAutoTracker
on (name = N'LegacyAutoTracker', filename = N'c:\data\sql2016\data\LegacyAutoTracker.mdf' , size = 10240kb , filegrowth = 10240kb )
log on (name = N'LegacyAutoTracker_log', filename = N'c:\data\sql2016\log\LegacyAutoTracker_log.ldf' , size = 10240kb , filegrowth = 10240kb )
go

use LegacyAutoTracker;
go

drop table if exists #CustomerAll;

create table #CustomerAll
(
	CustomerId int not null,
	FirstName varchar(20) not null,
	LastName varchar(20) not null,
	Address varchar(80) not null,
	City varchar(50) not null,
	State char(2) not null,
	ValidFrom datetime2 not null,
	ValidTo datetime2 not null,
	Guid uniqueidentifier not null default (newid())
);

create clustered index ix1_CustomerAll on #CustomerAll (CustomerId, ValidFrom);
create unique nonclustered index ux_CustomerAll on #CustomerAll (Guid);

drop table if exists LegacyCustomerHistory;

create table dbo.LegacyCustomerHistory
(
	ChangeId int not null identity(1,1),
	CustomerId int not null,
	ChangeType varchar(10) not null,
	FirstName varchar(20) not null,
	LastName varchar(20) not null,
	Address varchar(80) not null,
	City varchar(50) not null,
	State char(2) not null,
	ChangeTime datetime2 not null,
	constraint pk_LegacyCustomerHistory primary key clustered (ChangeId)
);

-- Get the history records.

insert #CustomerAll
 (CustomerId, FirstName, LastName, Address, City, State, ValidFrom, ValidTo)
select *
from AutoTracker.dbo.Customer for system_time all;

-- Stretch the time columns to appear that these changes were made over the course of
-- a few years rather than just one hour.

declare @earliestTime datetime2 = (select min(ValidFrom) from #CustomerAll);
declare @latestTime datetime2 = (select max(ValidTo) from #CustomerAll where ValidTo < '9999-12-31');
declare @testSpan int = datediff(millisecond, @earliestTime, @latestTime);

declare @start datetime2 = '2001-01-02 8:21:17.7234176';
declare @end datetime2 = '2017-05-31 16:52:29.9034701';
declare @span bigint = cast(datediff(second, @start, @end) as bigint) * 1000;

declare @ratio float = 1. * @span / @testSpan;
--select @span, @testSpan, @ratio;

update c
set ValidFrom = dateadd(second, cast(datediff(second, @earliestTime, c.ValidFrom) * @ratio as int), @start),
	ValidTo = case when c.ValidTo >= '9999-12-31' then c.ValidTo else dateadd(second, cast(datediff(second, @earliestTime, c.ValidTo) * @ratio as int), @start) end
from #CustomerAll c
where 0 = 0;

-- Create a worktable.

drop table if exists #t;

create table #t
(
	ChangeType varchar(10) not null,
	CustomerId int not null,
	FirstName varchar(20) not null,
	LastName varchar(20) not null,
	Address varchar(80) not null,
	City varchar(50) not null,
	State char(2) not null,
	ChangeTime datetime2 not null,
	Guid uniqueidentifier not null
);

-- Original inserts are earliest ValidFrom time for each CustomerId.

with InsertRecords as
(
	select c.CustomerId, c.FirstName, c.LastName, c.Address, c.City, c.State, c.ValidFrom, c.ValidTo, c.Guid,
		row_number() over (partition by c.CustomerId order by c.ValidFrom) rn
	from #CustomerAll c
)
insert #t (ChangeType, CustomerId, FirstName, LastName, Address, City, State, ChangeTime, Guid)
select 'Insert', i.CustomerId, i.FirstName, i.LastName, i.Address, i.City, i.State, i.ValidFrom, i.Guid
from InsertRecords i
where i.rn = 1;

-- Deletions are when the last ValidTo for a CustomerId is less than the end-of-history.
-- Deletion can be when there is a gap in history, but that shouldn't be the case here.

with DeleteRecords as
(
	select c.CustomerId, c.FirstName, c.LastName, c.Address, c.City, c.State, c.ValidFrom, c.ValidTo, c.Guid,
		row_number() over (partition by c.CustomerId order by c.ValidTo desc) rn
	from #CustomerAll c
)
insert #t (ChangeType, CustomerId, FirstName, LastName, Address, City, State, ChangeTime, Guid)
select 'Delete', d.CustomerId, d.FirstName, d.LastName, d.Address, d.City, d.State, d.ValidTo, d.Guid
from DeleteRecords d
where d.rn = 1
and d.ValidTo < '9999-12-31';

-- All others are updates

insert #t (ChangeType, CustomerId, FirstName, LastName, Address, City, State, ChangeTime, Guid)
select 'Update', c.CustomerId, c.FirstName, c.LastName, c.Address, c.City, c.State, c.ValidFrom, c.Guid
from #CustomerAll c
left join #t t on t.Guid = c.Guid
where t.CustomerId is null;

-- Let's get rid of most of the "insert-only" records

drop table if exists #SingletonRecords;

select CustomerId
into #SingletonRecords
from #t
group by CustomerId
having count(*) = 1;

drop table if exists #RecordsToKeep;

select top 1000 s.CustomerId
into #RecordsToKeep
from #SingletonRecords s
order by newid();

delete #t
where CustomerId in
(
	select CustomerId
	from #SingletonRecords s
	where s.CustomerId not in (select r.CustomerId from #RecordsToKeep r)
);

-- Add to legacy history.
insert dbo.LegacyCustomerHistory (CustomerId, ChangeType, FirstName, LastName, Address, City, State, ChangeTime)
select CustomerId, ChangeType, FirstName, LastName, Address, City, State, ChangeTime
from #t
order by ChangeTime, CustomerId;

select * from dbo.LegacyCustomerHistory order by ChangeId;
