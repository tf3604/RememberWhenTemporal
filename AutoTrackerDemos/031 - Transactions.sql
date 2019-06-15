-----------------------------------------------------------------------------------------------------------------------
-- 031 - Transactions.sql
-- Version 1.0.5
-- Look for the most recent version of this script at www.tf3604.com/temporal.
-- MIT License.  See the bottom of this file for details.
-----------------------------------------------------------------------------------------------------------------------
use AutoTracker2;
go

-- Run the entire query below.
begin transaction;

select sysutcdatetime() ApproxTransactionStartTime

waitfor delay '0:00:10';

insert dbo.Customer (FirstName, LastName, Address, City, State)
values ('Miguel', 'Jones', '6766 S Nyla Ct', 'Hollywood', 'FL');

declare @CustomerId int = scope_identity();

waitfor delay '0:00:05';

update dbo.Customer
set State = 'CA'
where CustomerId = @CustomerId;

waitfor delay '0:00:05';

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



-----------------------------------------------------------------------------------------------------------------------
-- Copyright 2017-2019, Brian Hansen (brian at tf3604.com).

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
