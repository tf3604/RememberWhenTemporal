----------------------------------------------------------------------------------------------------
-- Cleanup old database
----------------------------------------------------------------------------------------------------
use master;
go
if exists (select * from sys.databases where name = 'AutoTracker')
begin
	alter database AutoTracker set offline with rollback immediate;
	alter database AutoTracker set online with rollback immediate;
	drop database AutoTracker;
end
go

----------------------------------------------------------------------------------------------------
-- Create database.  Adjust paths as needed.
----------------------------------------------------------------------------------------------------
create database AutoTracker
on (name = N'AutoTracker', filename = N'c:\data\sql2016\data\AutoTracker.mdf' , size = 10240kb , filegrowth = 10240kb )
log on (name = N'AutoTracker_log', filename = N'c:\data\sql2016\log\AutoTracker_log.ldf' , size = 10240kb , filegrowth = 10240kb )
go

use AutoTracker;
go

----------------------------------------------------------------------------------------------------
-- Drop existing tables
----------------------------------------------------------------------------------------------------
if exists
(
	select t.name
	from sys.tables t
	inner join sys.schemas s on s.schema_id = t.schema_id
	where s.name = 'dbo'
	and t.name = 'Ownership'
	and t.temporal_type_desc = 'SYSTEM_VERSIONED_TEMPORAL_TABLE'
)
begin
	alter table dbo.Ownership set (system_versioning = off);
end

drop table if exists history.OwnershipHistory;
drop table if exists dbo.Ownership;
go

if exists
(
	select t.name
	from sys.tables t
	inner join sys.schemas s on s.schema_id = t.schema_id
	where s.name = 'dbo'
	and t.name = 'Dealer'
	and t.temporal_type_desc = 'SYSTEM_VERSIONED_TEMPORAL_TABLE'
)
begin
	alter table dbo.Dealer set (system_versioning = off);
end

drop table if exists history.DealerHistory;
drop table if exists dbo.Dealer;
go

if exists
(
	select t.name
	from sys.tables t
	inner join sys.schemas s on s.schema_id = t.schema_id
	where s.name = 'dbo'
	and t.name = 'Customer'
	and t.temporal_type_desc = 'SYSTEM_VERSIONED_TEMPORAL_TABLE'
)
begin
	alter table dbo.Customer set (system_versioning = off);
end

drop table if exists history.CustomerHistory;
drop table if exists dbo.Customer;
go

if exists
(
	select t.name
	from sys.tables t
	inner join sys.schemas s on s.schema_id = t.schema_id
	where s.name = 'dbo'
	and t.name = 'Automobile'
	and t.temporal_type_desc = 'SYSTEM_VERSIONED_TEMPORAL_TABLE'
)
begin
	alter table dbo.Automobile set (system_versioning = off);
end

drop table if exists history.AutomobileHistory;
drop table if exists dbo.Automobile;
go

if exists
(
	select t.name
	from sys.tables t
	inner join sys.schemas s on s.schema_id = t.schema_id
	where s.name = 'dbo'
	and t.name = 'AutoModel'
	and t.temporal_type_desc = 'SYSTEM_VERSIONED_TEMPORAL_TABLE'
)
begin
	alter table dbo.AutoModel set (system_versioning = off);
end

drop table if exists history.AutoModelHistory;
drop table if exists dbo.AutoModel;
go

declare @dropSql nvarchar(max);

select @dropSql = 'alter table ' + quotename(s.name) + '.' + quotename(t.name) + ' set (system_versioning = off); ' + 'drop table ' + quotename(hs.name) + '.' + quotename(ht.name)
from sys.tables t
inner join sys.schemas s on t.schema_id = s.schema_id
inner join sys.tables ht on t.history_table_id = ht.object_id
inner join sys.schemas hs on ht.schema_id = hs.schema_id
where t.name = 'Manufacturer'
and s.name = 'dbo';

exec(@dropSql);

drop table if exists dbo.Manufacturer;

drop table if exists history.CustomerArchive;
drop table if exists history.OwnershipArchive;

----------------------------------------------------------------------------------------------------
-- Create history schema
----------------------------------------------------------------------------------------------------
drop schema if exists history;
go
create schema history authorization dbo;

----------------------------------------------------------------------------------------------------
-- Create tables.
----------------------------------------------------------------------------------------------------
go
create table dbo.Manufacturer
(
	ManufacturerId int not null identity(1,1),
	ManufacturerName nvarchar(50) not null,
	ValidFrom datetime2 generated always as row start not null,
	ValidTo datetime2 generated always as row end not null,
	period for system_time (ValidFrom, ValidTo),
	constraint pk_Manufacturer primary key clustered (ManufacturerId)
)
with (system_versioning = on);

set identity_insert dbo.Manufacturer on;
insert dbo.Manufacturer (ManufacturerId, ManufacturerName)
values (1, 'FordoyotaBenz');
set identity_insert dbo.Manufacturer off;
go

create table dbo.AutoModel
(
	ModelId int not null identity(1,1),
	ManufacturerId int not null,
	ModelName nvarchar(50) not null,
	Description nvarchar(100) null,
	ValidFrom datetime2 generated always as row start not null,
	ValidTo datetime2 generated always as row end not null,
	period for system_time (ValidFrom, ValidTo),
	constraint pk_Model primary key clustered (ModelId),
	constraint fk_Model__ManufacturerId foreign key (ManufacturerId) references Manufacturer (ManufacturerId)
)
with (system_versioning = on (history_table = history.AutoModelHistory));
go
-- Load some data into dbo.AutoModel
insert dbo.AutoModel (ManufacturerId, ModelName, Description)
values (1, 'Squeeze', 'Mini'),
(1, 'Sipper', 'Compact'),
(1, 'Treaty', 'MidSize'),
(1, 'Pretention', 'FullSize'),
(1, 'Beast', 'Pickup'),
(1, 'KidTaxi', 'Small SUV'),
(1, 'Guzzler', 'Large SUV');
go
create table history.AutomobileHistory
(
	AutomobileId int not null,
	ModelId int not null,
	VIN nvarchar(50) not null,
	Color nvarchar(20) null,
	ValidFrom datetime2 not null,
	ValidTo datetime2 not null
);
go
create clustered index idx1_AutomobileHistory on history.AutomobileHistory (ValidTo, ValidFrom);
go
create table dbo.Automobile
(
	AutomobileId int not null identity(1,1),
	ModelId int not null,
	VIN nvarchar(50) not null,
	Color nvarchar(20) null,
	ValidFrom datetime2 generated always as row start not null,
	ValidTo datetime2 generated always as row end not null,
	period for system_time (ValidFrom, ValidTo),
	constraint pk_Automobile primary key clustered (AutomobileId),
	constraint fk_Automobile__ModelId foreign key (ModelId) references AutoModel (ModelId)
)
with (system_versioning = on (history_table = history.AutomobileHistory));
go
drop table if exists history.CustomerHistory;
go
create table history.CustomerHistory
(
	CustomerId int not null,
	FirstName varchar(20) not null,
	LastName varchar(20) not null,
	Address varchar(80) not null,
	City varchar(50) not null,
	State char(2) not null,
	ValidFrom datetime2 not null,
	ValidTo datetime2 not null,
	--WhoChanged sysname not null default (suser_sname())
);
go
create clustered index ix1_CustomerHistory on history.CustomerHistory (ValidTo, ValidFrom);
go
create table dbo.Customer
(
	CustomerId int not null identity(1,1),
	FirstName varchar(20) not null,
	LastName varchar(20) not null,
	Address varchar(80) not null,
	City varchar(50) not null,
	State char(2) not null,
	ValidFrom datetime2 generated always as row start not null,
	ValidTo datetime2 generated always as row end not null,
	period for system_time (ValidFrom, ValidTo),
	constraint pk_CustomerId primary key clustered (CustomerId)
)
with (system_versioning = on (history_table = history.CustomerHistory));
go
create table history.CustomerArchive
(
	CustomerId int not null,
	FirstName varchar(20) not null,
	LastName varchar(20) not null,
	Address varchar(80) not null,
	City varchar(50) not null,
	State char(2) not null,
	ValidFrom datetime2 not null,
	ValidTo datetime2 not null,
);
go
create clustered index ix1_CustomerArchive on history.CustomerArchive (ValidTo, ValidFrom);
go
create table dbo.Dealer
(
	DealerId int not null identity(1,1),
	Name nvarchar(50) not null,
	Address varchar(80) not null,
	City varchar(50) not null,
	State char(2) not null,
	ValidFrom datetime2 generated always as row start not null,
	ValidTo datetime2 generated always as row end not null,
	period for system_time (ValidFrom, ValidTo),
	constraint pk_DealerId primary key clustered (DealerId)
)
with (system_versioning = on (history_table = history.DealerHistory));
go
if exists (select * from sys.partition_schemes ps where ps.name = 'schemeOwnershipHistoryByEndTime')
	drop partition scheme schemeOwnershipHistoryByEndTime;
go
if exists (select * from sys.partition_schemes ps where ps.name = 'schemeOwnershipArchiveByEndTime')
	drop partition scheme schemeOwnershipArchiveByEndTime;
go
if exists (select * from sys.partition_functions pf where pf.name = 'fnOwnershipHistoryPartitionByEndTime')
	drop partition function fnOwnershipHistoryPartitionByEndTime;
go
if exists (select * from sys.partition_functions pf where pf.name = 'fnOwnershipArchivePartitionByEndTime')
	drop partition function fnOwnershipArchivePartitionByEndTime;
go
declare @now datetime2 = sysutcdatetime();
declare @nowPlus5 datetime2 = dateadd(minute, 5, @now);
declare @nowPlus10 datetime2 = dateadd(minute, 10, @now);
declare @nowPlus15 datetime2 = dateadd(minute, 15, @now);
declare @nowPlus20 datetime2 = dateadd(minute, 20, @now);
declare @nowPlus25 datetime2 = dateadd(minute, 25, @now);
declare @nowPlus30 datetime2 = dateadd(minute, 30, @now);
declare @nowPlus35 datetime2 = dateadd(minute, 35, @now);
declare @nowPlus40 datetime2 = dateadd(minute, 40, @now);
declare @nowPlus45 datetime2 = dateadd(minute, 45, @now);
declare @nowPlus50 datetime2 = dateadd(minute, 50, @now);
declare @nowPlus55 datetime2 = dateadd(minute, 55, @now);
declare @nowPlus60 datetime2 = dateadd(minute, 60, @now);
declare @nowPlus65 datetime2 = dateadd(minute, 65, @now);
declare @nowPlus70 datetime2 = dateadd(minute, 70, @now);
declare @nowPlus75 datetime2 = dateadd(minute, 75, @now);

create partition function fnOwnershipHistoryPartitionByEndTime (datetime2)
as range left for values (@now, @nowPlus5, @nowPlus10, @nowPlus15, @nowPlus20, @nowPlus25, @nowPlus30, @nowPlus35, @nowPlus40, @nowPlus45, @nowPlus50, @nowPlus55, @nowPlus60, @nowPlus65, @nowPlus70, @nowPlus75);

create partition function fnOwnershipArchivePartitionByEndTime (datetime2)
as range left for values (@now, @nowPlus5, @nowPlus10, @nowPlus15, @nowPlus20, @nowPlus25, @nowPlus30, @nowPlus35, @nowPlus40, @nowPlus45, @nowPlus50, @nowPlus55, @nowPlus60, @nowPlus65, @nowPlus70, @nowPlus75);
go
create partition scheme schemeOwnershipHistoryByEndTime
as partition fnOwnershipHistoryPartitionByEndTime
to ([primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary]);
go
create partition scheme schemeOwnershipArchiveByEndTime
as partition fnOwnershipArchivePartitionByEndTime
to ([primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary], [primary]);
go
create table history.OwnershipHistory
(
	OwnershipId int not null,
	AutomobileId int not null,
	DealerId int null,
	CustomerId int null,
	EffectiveDate date not null,
	ValidFrom datetime2 not null,
	ValidTo datetime2 not null
);
go
create clustered index idx_OwnershipHistory on history.OwnershipHistory (ValidTo, ValidFrom)
on schemeOwnershipHistoryByEndTime (ValidTo);
go
create table history.OwnershipArchive
(
	OwnershipId int not null,
	AutomobileId int not null,
	DealerId int null,
	CustomerId int null,
	EffectiveDate date not null,
	ValidFrom datetime2 not null,
	ValidTo datetime2 not null
);
go
create clustered index idx_OwnershipArchive on history.OwnershipArchive (ValidTo, ValidFrom)
on schemeOwnershipArchiveByEndTime (ValidTo);
go
create table dbo.Ownership
(
	OwnershipId int not null identity(1,1),
	AutomobileId int not null,
	DealerId int null,
	CustomerId int null,
	EffectiveDate date not null,
	ValidFrom datetime2 generated always as row start not null,
	ValidTo datetime2 generated always as row end not null,
	period for system_time (ValidFrom, ValidTo),
	constraint pk_Ownership primary key clustered (OwnershipId),
	constraint fk_Ownership__AutomobileId foreign key (AutomobileId) references Automobile (AutomobileId),
	constraint fk_Ownership__DealerId foreign key (DealerId) references Dealer (DealerId),
	constraint fk_Ownership__CustomerId foreign key (CustomerId) references Customer (CustomerId),
	constraint ck_Ownership__Target check
		((DealerId is null and CustomerId is not null) or
		(DealerId is not null and CustomerId is null))
)
with (system_versioning = on (history_table = history.OwnershipHistory));

create unique nonclustered index ix1_Ownership__AutomobileId on dbo.Ownership (AutomobileId);
