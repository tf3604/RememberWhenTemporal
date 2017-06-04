use master;
go
if exists (select * from sys.databases where name = 'AutoTracker')
begin
	alter database AutoTracker set offline with rollback immediate;
	alter database AutoTracker set online with rollback immediate;
	drop database AutoTracker;
end
go

-- Create database.  Adjust paths as needed.
create database AutoTracker
on (name = N'AutoTracker', filename = N'c:\data\sql2016\data\AutoTracker.mdf' , size = 10240kb , filegrowth = 10240kb )
log on (name = N'AutoTracker_log', filename = N'c:\data\sql2016\log\AutoTracker_log.ldf' , size = 10240kb , filegrowth = 10240kb )
GO

-- Create tables.
use AutoTracker;
go

drop schema if exists history;
go
create schema history authorization dbo;

go
drop table if exists dbo.Manufacturer;
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

select	s.name source_table_schema,
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

set identity_insert dbo.Manufacturer on;
insert dbo.Manufacturer (ManufacturerId, ManufacturerName)
values (1, 'FordoyotaBenz');
set identity_insert dbo.Manufacturer off;
go

declare @dropSql nvarchar(max);

select @dropSql = 'alter table ' + quotename(s.name) + '.' + quotename(t.name) + ' set (system_versioning = off); ' + 'drop table ' + quotename(hs.name) + '.' + quotename(ht.name)
from sys.tables t
inner join sys.schemas s on t.schema_id = s.schema_id
inner join sys.tables ht on t.history_table_id = ht.object_id
inner join sys.schemas hs on ht.schema_id = hs.schema_id
where t.name = 'AutoModel'
and s.name = 'dbo';

exec(@dropSql);

drop table if exists dbo.AutoModel;
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

select	s.name source_table_schema,
		t.name source_table_name,
		t.temporal_type_desc,
		hs.name history_table_schema,
		ht.name history_table_name
from sys.tables t
inner join sys.schemas s on t.schema_id = s.schema_id
inner join sys.tables ht on t.history_table_id = ht.object_id
inner join sys.schemas hs on ht.schema_id = hs.schema_id
where t.name = 'AutoModel'
and s.name = 'dbo';
go

insert dbo.AutoModel (ManufacturerId, ModelName, Description)
values (1, 'Squeeze', 'Mini'),
(1, 'Sipper', 'Compact'),
(1, 'Treaty', 'MidSize'),
(1, 'Pretention', 'FullSize'),
(1, 'Beast', 'Pickup'),
(1, 'KidTaxi', 'Small SUV'),
(1, 'Guzzler', 'Large SUV');
go

drop table if exists dbo.Automobile;
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

drop table if exists dbo.Customer;
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

drop table if exists dbo.Dealer;
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

drop table if exists dbo.Ownership;
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
with (system_versioning = on (history_table = history.Ownership));

