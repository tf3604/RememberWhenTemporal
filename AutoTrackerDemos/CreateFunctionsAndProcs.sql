use AutoTracker;
go
drop procedure if exists utility.spGenerateRandomCustomers;
drop procedure if exists utility.spUpdateRandomCustomer;
drop procedure if exists utility.spGenerateRandomDealers;
drop procedure if exists utility.spUpdateRandomDealer;
drop procedure if exists utility.spCreateRandomAutomobiles;
drop table if exists utility.WorkloadDriver;
go
drop schema if exists utility;
go
create schema utility authorization dbo;
go
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
create procedure utility.spUpdateRandomCustomer
as
declare @customerCount int = (select count(*) from dbo.Customer);
declare @randomNumber int = cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @customerCount as int);
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
create procedure utility.spUpdateRandomDealer
as
declare @dealerCount int = (select count(*) from dbo.Dealer);
declare @randomNumber int = cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @dealerCount as int);
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
create procedure utility.spCreateRandomAutomobiles
(
	@automobilesToCreate int
)
as
declare @modelCount int = (select count(*) from dbo.AutoModel);

with RandomValues as
(
	select	cast((binary_checksum(newid()) + 2147483648.) / 4294967296. * @modelCount + 1 as int) r1
	from	bkhUtility.utility.fnNums() n
	where	n.n <= @automobilesToCreate
), ModelSk as
(
	select ModelId,
		row_number() over (order by ModelId) Sk
	from dbo.AutoModel
)
insert dbo.Automobile (ModelId, VIN)
select sk.ModelId, replace(lower(cast(newid() as nvarchar(50))), '-', '') VIN
from RandomValues rv
cross apply (select sk.ModelId from ModelSk sk where sk.Sk = rv.r1) sk
go
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
	select rw.ID, rw.Weight, isnull(sum(rw.RelativeWeight) over (order by ID rows between unbounded preceding and 1 preceding), 0) RelativeWeightRunningTotal
	from RelativeWorkload rw
)
update rt
set Weight = rt.RelativeWeightRunningTotal
from RunningTotal rt
where 1 = 1;
go

