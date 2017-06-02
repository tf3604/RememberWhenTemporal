-- Create database.  Adjust paths as needed.
create database AutoTracker
on (name = N'AutoTracker', filename = N'c:\data\sql2016\data\AutoTracker.mdf' , size = 10240kb , filegrowth = 10240kb )
log on (name = N'AutoTracker_log', filename = N'c:\data\sql2016\log\AutoTracker_log.ldf' , size = 10240kb , filegrowth = 10240kb )
GO

-- Create tables.
use AutoTracker;
go

drop table if exists Customer;
go
create table Customer
(
	CustomerId int not null identity(1,1),
	constraint pk_CustomerId primary key clustered (CustomerId)
);
go

drop table if exists Dealer;
go
create table Dealer
(
	DealerId int not null identity(1,1),
	constraint pk_DealerId primary key clustered (DealerId)
);
go

drop table if exists OwnershipTransfer;
go
create table OwnershipTransfer
(
	OwnershipTransferId int not null identity(1,1),
	constraint pk_OwnershipTransferId primary key clustered (OwnershipTransferId)
);

