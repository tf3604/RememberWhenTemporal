use AutoTracker2;
go

-- Run the entire query below.
begin transaction;

select sysutcdatetime() ApproxTransactionStartTime

waitfor delay '0:00:10';

insert dbo.Customer (FirstName, LastName, Address, City, State)
values ('Miguel', 'Jones', '6766 S Nyla Ct', 'Hollywood', 'FL');

declare @CustomerId int = scope_identity();

waitfor delay '0:00:10';

insert dbo.Automobile (ModelId, VIN)
values (1, replace(lower(cast(newid() as nvarchar(50))), '-', ''));

declare @AutomobileId int = scope_identity();

waitfor delay '0:00:10';

insert dbo.Ownership (AutomobileId, DealerId, CustomerId, EffectiveDate)
values (@AutomobileId, null, @CustomerId, '2017-05-15');

declare @OwnershipId int = scope_identity();

commit transaction;

select * from dbo.Customer where CustomerId = @CustomerId;
select * from dbo.Automobile where AutomobileId = @AutomobileId;
select * from dbo.Ownership where OwnershipId = @OwnershipId;
