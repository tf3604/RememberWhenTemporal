-----------------------------------------------------------------------------------------------------------------------
-- 080 - ConvertExistingHistory.sql
-- Version 1.0.5
-- Look for the most recent version of this script at www.tf3604.com/temporal.
-- MIT License.  See the bottom of this file for details.
-----------------------------------------------------------------------------------------------------------------------

use AutoTracker;
go

-- The LegacyCustomerHistory table (LegacyAutoTracker database) contains historical data
-- from the application we are retiring.  We want to convert it to a temporal table.
-- This table contains the full history, and each row indicates whether it resulted
-- from an insert, delete or update operation.

-- Example record:
select *
from LegacyAutoTracker.dbo.LegacyCustomerHistory
where CustomerId = 2553
order by ChangeTime;

-- We'll create a new temporal table and the corresponding history table, but
-- we'll keep them detached for the moment.
drop table if exists history.ConvertedCustomerHistory;
go
create table history.ConvertedCustomerHistory
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
create clustered index ix1_ConvertedCustomerHistory on history.ConvertedCustomerHistory (ValidTo, ValidFrom);
go
drop table if exists dbo.ConvertedCustomer;
go
create table dbo.ConvertedCustomer
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
	constraint pk_ConvertedCustomerId primary key clustered (CustomerId)
)
with (system_versioning = off);
go

-- Well find the "current" records.  If the most recent record for a given CustomerId is an
-- Insert or Update operation, this should be the current record.
set identity_insert dbo.ConvertedCustomer on;

with CurrentRecords as
(
	select lch.ChangeId, lch.CustomerId, lch.ChangeType, lch.FirstName, lch.LastName, lch.Address, lch.City, lch.State, lch.ChangeTime,
		row_number () over (partition by lch.CustomerId order by lch.ChangeTime desc) rn
	from LegacyAutoTracker.dbo.LegacyCustomerHistory lch
)
insert dbo.ConvertedCustomer (CustomerId, FirstName, LastName, Address, City, State)
select c.CustomerId, c.FirstName, c.LastName, c.Address, c.City, c.State
from CurrentRecords c
where c.rn = 1
and c.ChangeType in ('Insert', 'Update');

set identity_insert dbo.ConvertedCustomer off;

-- Note that we have no control over the "ValidFrom" column above -- it will be the current time.
-- So we'll insert a history record with the same core column values and a period of validity
-- from the legacy time through the current time.

with CurrentRecords as
(
	select lch.ChangeId, lch.CustomerId, lch.ChangeType, lch.FirstName, lch.LastName, lch.Address, lch.City, lch.State, lch.ChangeTime,
		row_number () over (partition by lch.CustomerId order by lch.ChangeTime desc) rn
	from LegacyAutoTracker.dbo.LegacyCustomerHistory lch
)
insert history.ConvertedCustomerHistory (CustomerId, FirstName, LastName, Address, City, State, ValidFrom, ValidTo)
select cc.CustomerId, cc.FirstName, cc.LastName, cc.Address, cc.City, cc.State, c.ChangeTime, cc.ValidFrom
from dbo.ConvertedCustomer cc
inner join CurrentRecords c on cc.CustomerId = c.CustomerId
where c.rn = 1
and c.ChangeType in ('Insert', 'Update');

-- Now we'll insert all remaining records into the history table.
-- Don't add the initial insert into the history table.
with CurrentRecords as
(
	select lch.ChangeId, lch.CustomerId, lch.ChangeType, lch.FirstName, lch.LastName, lch.Address, lch.City, lch.State, lch.ChangeTime,
		lag(lch.ChangeTime) over (partition by lch.CustomerId order by lch.ChangeTime) PreviousChangeTime,
		row_number () over (partition by lch.CustomerId order by lch.ChangeTime desc) rn
	from LegacyAutoTracker.dbo.LegacyCustomerHistory lch
)
insert history.ConvertedCustomerHistory (CustomerId, FirstName, LastName, Address, City, State, ValidFrom, ValidTo)
select c.CustomerId, c.FirstName, c.LastName, c.Address, c.City, c.State, c.PreviousChangeTime, c.ChangeTime
from CurrentRecords c
where (c.rn > 1 and c.ChangeType != 'Insert')
order by CustomerId;

-- Finally, associate the constructed temporal table and history tables.
-- Also run a consistency check to validate what we've done.
alter table dbo.ConvertedCustomer
set (system_versioning = on (history_table = history.ConvertedCustomerHistory, data_consistency_check = on));

-- Check the results.
-- Notice that the last two rows are the same.
select *
from dbo.ConvertedCustomer for system_time all c
where c.CustomerId = 2553
order by c.ValidFrom;



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
