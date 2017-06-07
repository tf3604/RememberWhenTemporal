use AutoTracker;
go

-- Find an ownership record that has changed
select top 1 OwnershipId, ValidFrom, ValidTo,
	dateadd(millisecond, datediff(millisecond, ValidFrom, ValidTo), ValidFrom) ValidMidPoint
from history.OwnershipHistory
where CustomerId is not null
order by OwnershipId;

-- Copy the OwnershipId into the queries below
-- Copy the ValidMidpoint into the queries below

-- If we want to find the state of the system at a previous point in time, we need to apply
-- the same system_time to be consistent.
select o.OwnershipId, o.EffectiveDate, a.AutomobileId, a.ModelId, a.VIN, a.Color, c.CustomerId, c.FirstName, c.LastName, c.Address, c.City, c.State
from dbo.Ownership for system_time as of '2017-06-06 20:32:46.4561363' o
inner join dbo.Automobile for system_time as of '2017-06-06 20:32:46.4561363' a on a.AutomobileId = o.AutomobileId
inner join dbo.Customer for system_time as of '2017-06-06 20:32:46.4561363' c on c.CustomerId = o.CustomerId
where o.OwnershipId = 18;

-- That's cumbersome and error prone.  You would think this would work:
with Info as
(
	select o.OwnershipId, o.EffectiveDate, a.AutomobileId, a.ModelId, a.VIN, a.Color, c.CustomerId, c.FirstName, c.LastName, c.Address, c.City, c.State
	from dbo.Ownership o
	inner join dbo.Automobile a on a.AutomobileId = o.AutomobileId
	inner join dbo.Customer c on c.CustomerId = o.CustomerId
)
select Info.OwnershipId, Info.EffectiveDate, Info.AutomobileId, Info.ModelId, Info.VIN, Info.Color, CustomerId, Info.FirstName, Info.LastName, Info.Address, Info.City, Info.State
from Info for system_time as of '2017-06-06 20:32:46.4561363'
where OwnershipId = 18;
--Msg 13544, Level 16, State 1, Line 29
--Temporal FOR SYSTEM_TIME clause can only be used with system-versioned tables. 'Info' is not a system-versioned table.

-- Well, this probably won't work either:
select *
from
(
	select o.OwnershipId, o.EffectiveDate, a.AutomobileId, a.ModelId, a.VIN, a.Color, c.CustomerId, c.FirstName, c.LastName, c.Address, c.City, c.State
	from dbo.Ownership o
	inner join dbo.Automobile a on a.AutomobileId = o.AutomobileId
	inner join dbo.Customer c on c.CustomerId = o.CustomerId
) Info for system_time as of '2017-06-06 20:32:46.4561363'
where OwnershipId = 18;
--Msg 102, Level 15, State 1, Line 42
--Incorrect syntax near 'for'.

-- Try creating a view
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

-- And then query the view:
select o.OwnershipId, o.EffectiveDate, o.AutomobileId, o.ModelId, o.VIN, o.Color, o.CustomerId, o.FirstName, o.LastName, o.Address, o.City, o.State
from dbo.vwCustomerOwnership for system_time as of '2017-06-06 20:32:46.4561363' o
where OwnershipId = 18;
