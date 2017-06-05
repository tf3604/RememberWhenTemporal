use AutoTracker;
go

exec utility.spGenerateRandomCustomers 1000;

backup database bkhUtility to disk = 'C:\data\dev\Repos\TemporalPresentation\bkhUtility.bak' with compression;

select * from dbo.Customer;

begin transaction;

insert dbo.Customer (FirstName, LastName, Address, City, State)
values ('Miguel', 'Jones', '6766 S Nyla Ct', 'Hollywood', 'FL');

declare @CustomerId int = scope_identity();

waitfor delay '0:00:15';

insert dbo.Automobile (ModelId, VIN)
values (1, replace(lower(cast(newid() as nvarchar(50))), '-', ''));

declare @AutomobileId int = scope_identity();

waitfor delay '0:00:15';

insert dbo.Ownership (AutomobileId, DealerId, CustomerId, EffectiveDate)
values (@AutomobileId, null, @CustomerId, '2017-05-15');

declare @OwnershipId int = scope_identity();

commit transaction;

select * from dbo.Customer where CustomerId = @CustomerId;
select * from dbo.Automobile where AutomobileId = @AutomobileId;
select * from dbo.Ownership where OwnershipId = @OwnershipId;

delete dbo.Ownership where OwnershipId = 2;
delete dbo.Customer where CustomerId = 1004;
delete dbo.Automobile where AutomobileId = 4;

select * from dbo.Customer where CustomerId = 1004;
select * from dbo.Automobile where AutomobileId = 4;
select * from dbo.Ownership where OwnershipId = 2;

select * from history.CustomerHistory where CustomerId = 1004;
select * from history.AutomobileHistory where AutomobileId = 4;
select * from history.OwnershipHistory where OwnershipId = 2;

select o.OwnershipId, o.EffectiveDate, a.AutomobileId, a.ModelId, a.VIN, a.Color, c.CustomerId, c.FirstName, c.LastName, c.Address, c.City, c.State
from dbo.Ownership for system_time as of '2017-06-05 18:49:00.0000000' o
inner join dbo.Automobile for system_time as of '2017-06-05 18:49:00.0000000' a on a.AutomobileId = o.AutomobileId
inner join dbo.Customer for system_time as of '2017-06-05 18:49:00.0000000' c on c.CustomerId = o.CustomerId
where a.AutomobileId = 4

with Info as
(
	select o.OwnershipId, o.EffectiveDate, a.AutomobileId, a.ModelId, a.VIN, a.Color, c.CustomerId, c.FirstName, c.LastName, c.Address, c.City, c.State
	from dbo.Ownership o
	inner join dbo.Automobile a on a.AutomobileId = o.AutomobileId
	inner join dbo.Customer c on c.CustomerId = o.CustomerId
)
select *
from Info for system_time as of '2017-06-05 18:49:00.0000000'
where AutomobileId = 4

go
drop view if exists vwCustomerOwnership;
go
create view vwCustomerOwnership
as
select o.OwnershipId, o.EffectiveDate, a.AutomobileId, a.ModelId, a.VIN, a.Color, c.CustomerId, c.FirstName, c.LastName, c.Address, c.City, c.State
from dbo.Ownership o
inner join dbo.Automobile a on a.AutomobileId = o.AutomobileId
inner join dbo.Customer c on c.CustomerId = o.CustomerId;

go

select *
from dbo.vwCustomerOwnership for system_time as of '2017-06-05 18:49:00.0000000'
where AutomobileId = 4;
