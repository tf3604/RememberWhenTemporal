-----------------------------------------------------------------------------------------------------------------------
-- 011 - CreateFunctionsAndProcs.sql
-- Version 1.0.0
-- Look for the most recent version of this script at www.tf3604.com/temporal.
-- MIT License.  See the bottom of this file for details.
-----------------------------------------------------------------------------------------------------------------------

use AutoTracker;
go
drop procedure if exists utility.spGenerateRandomCustomers;
drop procedure if exists utility.spUpdateRandomCustomer;
drop procedure if exists utility.spGenerateRandomDealers;
drop procedure if exists utility.spUpdateRandomDealer;
drop procedure if exists utility.spCreateRandomAutomobiles;
drop procedure if exists utility.spDeleteRandomAutomobile;
drop procedure if exists utility.spDealerSaleToCustomer;
drop procedure if exists utility.spDealerPurchaseFromCustomer;
drop procedure if exists utility.spCustomerSaleToCustomer;
drop procedure if exists utility.spExecuteRandomWorkloadAction

drop table if exists utility.WorkloadDriver;
go
drop schema if exists utility;
go
create schema utility authorization dbo;
go
------------------------------------------------------------------------------------------------------------
create procedure utility.spGenerateRandomCustomers
(
	@customersToGenerate int
)
as
 
with RandomValues as
(
	select	(binary_checksum(newid()) + 2147483648.) / 4294967296. r1,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r2,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r3,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r4,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r5,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r6,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r7
	from	bkhUtility.utility.fnNums() n
	where	n.n <= @customersToGenerate
)
insert dbo.Customer (FirstName, LastName, Address, City, State)
select cust.FirstName, cust.LastName, cust.Address, cust.City, cust.State
from RandomValues
cross apply bkhUtility.utility.fnGenerateRandomCustomer(r1, r2, r3, r4, r5, r6, r7) cust;
go
------------------------------------------------------------------------------------------------------------
create procedure utility.spGenerateRandomDealers
(
	@dealersToGenerate int
)
as

with RandomValues as
(
	select	(binary_checksum(newid()) + 2147483648.) / 4294967296. r1,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r2,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r3,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r4,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r5,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r6,
			(binary_checksum(newid()) + 2147483648.) / 4294967296. r7
	from	bkhUtility.utility.fnNums() n
	where	n.n <= @dealersToGenerate
)
insert dbo.Dealer (Name, Address, City, State)
select 'Dealer ' + cast(cast(r1 * 1000000 as int) as nvarchar(20)), cust.Address, cust.City, cust.State
from RandomValues
cross apply bkhUtility.utility.fnGenerateRandomCustomer(r1, r2, r3, r4, r5, r6, r7) cust;
go
------------------------------------------------------------------------------------------------------------
create procedure utility.spUpdateRandomCustomer
as
declare @customerCount int = (select count(*) from dbo.Customer);
declare @randomNumber int = cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @customerCount + 1 as int);
declare @customerId int;

with CustomerSk as
(
	select CustomerId,
		row_number() over (order by CustomerId) Sk
	from dbo.Customer
)
select @customerId = CustomerId
from CustomerSk
where Sk = @randomNumber;

update c
set c.Address = cust.Address,
	c.City = cust.City,
	c.State = cust.State
from dbo.Customer c
cross apply bkhUtility.utility.fnGenerateRandomCustomer(
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.) cust
where c.CustomerId = @customerId
go
------------------------------------------------------------------------------------------------------------
create procedure utility.spUpdateRandomDealer
as
declare @dealerCount int = (select count(*) from dbo.Dealer);
declare @randomNumber int = cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @dealerCount + 1 as int);
declare @dealerId int;

with DealerSk as
(
	select DealerId,
		row_number() over (order by DealerId) Sk
	from dbo.Dealer
)
select @dealerId = DealerId
from DealerSk
where Sk = @randomNumber;

update d
set d.Address = dlr.Address
from dbo.Dealer d
cross apply bkhUtility.utility.fnGenerateRandomCustomer(
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.,
	(binary_checksum(newid()) + 2147483648.) / 4294967296.) dlr
where d.DealerId = @dealerId
go
------------------------------------------------------------------------------------------------------------
create procedure utility.spCreateRandomAutomobiles
(
	@automobilesToCreate int
)
as
declare @modelCount int = (select count(*) from dbo.AutoModel);
declare @dealerCount int = (select count(*) from dbo.Dealer);
declare @autoList table (AutomobileId int, DealerId int, EffectiveDate date);
declare @earliestTransferDate date = '2010-01-01';
declare @dayCount int = datediff(day, @earliestTransferDate, getdate());

begin transaction;

with RandomValues as
(
	select	cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @modelCount + 1 as int) r1,
		cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @modelCount + 1 as int) r2,
		cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @dayCount as int) r3
	from	bkhUtility.utility.fnNums() n
	where	n.n <= @automobilesToCreate
), ModelSk as
(
	select ModelId,
		row_number() over (order by ModelId) Sk
	from dbo.AutoModel
), DealerSk as
(
	select DealerId,
		row_number() over (order by DealerId) Sk
	from dbo.Dealer
)
merge dbo.Automobile a
using
(
	select msk.ModelId, replace(lower(cast(newid() as nvarchar(50))), '-', '') VIN, dsk.DealerId, dateadd(day, rv.r3, @earliestTransferDate) EffectiveDate
	from RandomValues rv
	cross apply (select msk.ModelId from ModelSk msk where msk.Sk = rv.r1) msk
	cross apply (select dsk.DealerId from DealerSk dsk where dsk.Sk = rv.r2) dsk
) src
on (0 = 1)
when not matched then
insert (ModelId, VIN)
values (src.ModelId, src.VIN)
output inserted.AutomobileId, src.DealerId, src.EffectiveDate
into @autoList;

insert dbo.Ownership (AutomobileId, DealerId, EffectiveDate)
select a.AutomobileId, a.DealerId, a.EffectiveDate
from @autoList a;

commit transaction;
go
------------------------------------------------------------------------------------------------------------
create procedure utility.spDeleteRandomAutomobile
as
declare @autoCount int = (select count(*) from dbo.Automobile);
declare @randomNumber int = cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @autoCount + 1 as int);
declare @autoId int;

begin transaction;

with AutoSk as
(
	select AutomobileId,
		row_number() over (order by AutomobileId) Sk
	from dbo.Automobile
)
select @autoId = AutomobileId
from AutoSk
where Sk = @randomNumber;

delete dbo.Ownership
where AutomobileId = @autoId;

delete dbo.Automobile
where AutomobileId = @autoId;

commit transaction;
go
------------------------------------------------------------------------------------------------------------
create procedure utility.spDealerSaleToCustomer
as
declare @autoCount int = (select count(*) from dbo.Automobile a inner join dbo.Ownership o on o.AutomobileId = a.AutomobileId where o.DealerId is not null);
declare @randomNumber int = cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @autoCount + 1 as int);
declare @earliestTransferDate date = '2010-01-01';
declare @dayCount int = datediff(day, @earliestTransferDate, getdate());
declare @effectiveDate date = dateadd(day, cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @dayCount as int), @earliestTransferDate);
declare @autoId int;

with AutoSk as
(
	select a.AutomobileId,
		row_number() over (order by a.AutomobileId) Sk
	from dbo.Automobile a
	inner join dbo.Ownership o on o.AutomobileId = a.AutomobileId
	where o.DealerId is not null
)
select @autoId = AutomobileId
from AutoSk
where Sk = @randomNumber;

declare @customerCount int = (select count(*) from dbo.Customer);
select @randomNumber = cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @customerCount + 1 as int);
declare @customerId int;

with CustomerSk as
(
	select CustomerId,
		row_number() over (order by CustomerId) Sk
	from dbo.Customer
)
select @customerId = CustomerId
from CustomerSk
where Sk = @randomNumber;

update dbo.Ownership
set CustomerId = @customerId,
	DealerId = null,
	EffectiveDate = @effectiveDate
where AutomobileId = @autoId;
go
------------------------------------------------------------------------------------------------------------
create procedure utility.spDealerPurchaseFromCustomer
as
declare @autoCount int = (select count(*) from dbo.Automobile a inner join dbo.Ownership o on o.AutomobileId = a.AutomobileId where o.DealerId is not null);
declare @randomNumber int = cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @autoCount + 1 as int);
declare @earliestTransferDate date = '2010-01-01';
declare @dayCount int = datediff(day, @earliestTransferDate, getdate());
declare @effectiveDate date = dateadd(day, cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @dayCount as int), @earliestTransferDate);
declare @autoId int;

with AutoSk as
(
	select a.AutomobileId,
		row_number() over (order by a.AutomobileId) Sk
	from dbo.Automobile a
	inner join dbo.Ownership o on o.AutomobileId = a.AutomobileId
	where o.DealerId is not null
)
select @autoId = AutomobileId
from AutoSk
where Sk = @randomNumber;

declare @dealerCount int = (select count(*) from dbo.Dealer);
select @randomNumber = cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @dealerCount + 1 as int);
declare @dealerId int;

with DealerSk as
(
	select DealerId,
		row_number() over (order by DealerId) Sk
	from dbo.Dealer
)
select @dealerId = DealerId
from DealerSk
where Sk = @randomNumber;

update dbo.Ownership
set CustomerId = null,
	DealerId = @dealerId,
	EffectiveDate = @effectiveDate
where AutomobileId = @autoId;
go
------------------------------------------------------------------------------------------------------------
create procedure utility.spCustomerSaleToCustomer
as
declare @autoCount int = (select count(*) from dbo.Automobile a inner join dbo.Ownership o on o.AutomobileId = a.AutomobileId where o.CustomerId is not null);
declare @randomNumber int = cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @autoCount + 1 as int);
declare @earliestTransferDate date = '2010-01-01';
declare @dayCount int = datediff(day, @earliestTransferDate, getdate());
declare @effectiveDate date = dateadd(day, cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @dayCount as int), @earliestTransferDate);
declare @autoId int;
declare @ownershipId int;
declare @sourceCustomerId int;

with AutoSk as
(
	select a.AutomobileId, o.CustomerId SourceCustomerId, o.OwnerShipId,
		row_number() over (order by a.AutomobileId) Sk
	from dbo.Automobile a
	inner join dbo.Ownership o on o.AutomobileId = a.AutomobileId
	where o.CustomerId is not null
)
select @autoId = AutomobileId,
	@ownershipId = AutoSk.OwnershipId,
	@sourceCustomerId = AutoSk.SourceCustomerId
from AutoSk
where Sk = @randomNumber;

declare @customerCount int = (select count(*) from dbo.Customer where CustomerId != @sourceCustomerId);
select @randomNumber = cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @customerCount + 1 as int);
declare @targetCustomerId int;

with CustomerSk as
(
	select CustomerId,
		row_number() over (order by CustomerId) Sk
	from dbo.Customer
	where CustomerId != @sourceCustomerId
)
select @targetCustomerId = CustomerId
from CustomerSk
where Sk = @randomNumber;

update dbo.Ownership
set CustomerId = @targetCustomerId,
	EffectiveDate = @effectiveDate
where OwnershipId = @ownershipId;
go
------------------------------------------------------------------------------------------------------------
create table utility.WorkloadDriver
(
	ID int not null identity(1,1),
	Description nvarchar(50) not null,
	Code nvarchar(10) not null,
	RawWeight float not null,
	Weight float null,
	constraint pk_WorkloadDriver primary key clustered (ID)
);
go
insert utility.WorkloadDriver (Description, Code, RawWeight)
values ('Create customer', 'CRCUST', 1000),
('Create dealer', 'CRDEAL', 10),
('Create auto', 'CRAUTO', 500),
('Delete auto', 'DELAUTO', 50),
('Update customer address', 'UPCUSTADDR', 100),
('Update dealer address', 'UPDEALADDR', 1),
('Dealer sale', 'SALEDEAL', 300),
('Private sale', 'SALEPRIV', 200),
('Dealer purchase', 'DEALBUY', 100);
go
with Totals as
(
	select sum(RawWeight) Weight
	from utility.WorkloadDriver
), RelativeWorkload as
(
	select wd.ID, wd.Weight, wd.RawWeight, wd.RawWeight / t.Weight RelativeWeight
	from utility.WorkloadDriver wd
	cross join Totals t
), RunningTotal as
(
	select rw.ID, rw.Weight, isnull(sum(rw.RelativeWeight) over (order by ID rows unbounded preceding), 0) RelativeWeightRunningTotal
	from RelativeWorkload rw
)
update rt
set Weight = rt.RelativeWeightRunningTotal
from RunningTotal rt
where 1 = 1;
go
------------------------------------------------------------------------------------------------------------
create procedure utility.spExecuteRandomWorkloadAction
as
declare @actionCode nvarchar(10);
declare @randValue float = (binary_checksum(newid()) + 2147483648.) / 4294967296.;

select top 1 @actionCode = wd.Code
from utility.WorkloadDriver wd
where wd.Weight >= @randValue
order by wd.Weight;

if @actionCode = 'CRCUST'
	exec utility.spGenerateRandomCustomers 1;
else if @actionCode = 'CRDEAL'
	exec utility.spGenerateRandomDealers 1;
else if @actionCode = 'CRAUTO'
	exec utility.spCreateRandomAutomobiles 1;
else if @actionCode = 'DELAUTO'
	exec utility.spDeleteRandomAutomobile;
else if @actionCode = 'UPCUSTADDR'
	exec utility.spUpdateRandomCustomer;
else if @actionCode = 'UPDEALADDR'
	exec utility.spUpdateRandomDealer;
else if @actionCode = 'SALEDEAL'
	exec utility.spDealerSaleToCustomer;
else if @actionCode = 'SALEPRIV'
	exec utility.spCustomerSaleToCustomer;
else if @actionCode = 'DEALBUY'
	exec utility.spDealerPurchaseFromCustomer;
go



-----------------------------------------------------------------------------------------------------------------------
-- Copyright 2017, Brian Hansen (brian at tf3604.com).

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
