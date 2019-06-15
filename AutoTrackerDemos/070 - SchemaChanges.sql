-----------------------------------------------------------------------------------------------------------------------
-- 070 - SchemaChanges.sql
-- Version 1.0.0
-- Look for the most recent version of this script at www.tf3604.com/temporal.
-- MIT License.  See the bottom of this file for details.
-----------------------------------------------------------------------------------------------------------------------

use AutoTracker2;
go

-- We didn't include a postal code in our customer table.
select * from dbo.Customer;

-- So naturally there isn't one in the history table either.
select * from history.CustomerHistory;

-- What heppens if we alter the customer table?
-- Let's make it mandatory.
alter table dbo.Customer
add PostalCode nvarchar(20) not null;
--Msg 4901, Level 16, State 1, Line 14
--ALTER TABLE only allows columns to be added that can contain nulls, or have a DEFAULT definition specified, or the column being added is an identity or timestamp column, or alternatively if none of the previous conditions are satisfied the table must be empty to allow addition of this column. Column 'PostalCode' cannot be added to non-empty table 'CustomerHistory' because it does not satisfy these conditions.

-- Nothing new here, that would have failed on a non-temporal table as well.
-- Let's give it a default.
alter table dbo.Customer
add PostalCode nvarchar(20) not null default ('00000');

-- Check the results.
select * from dbo.Customer;
select * from history.CustomerHistory;

-- The default values pass down to the history table as well.

-- Let's add a nullable column
alter table dbo.Customer
add EmailAddress nvarchar(50) null;

-- That worked.  Check the results.
select * from dbo.Customer;
select * from history.CustomerHistory;

-- Now let's update the record.
update dbo.Customer
set EmailAddress = 'mjones@emailprovider.com'
where CustomerId = 1;

-- So far, so good.
select * from dbo.Customer;
select * from history.CustomerHistory;

-- Let's add another column.  Initially set it null, populate the values, then set it to not null.
alter table dbo.Customer
add CellPhone nvarchar(20) null;

update dbo.Customer
set CellPhone = 'Unknown';

alter table dbo.Customer
alter column CellPhone varchar(20) not null;
--Msg 515, Level 16, State 2, Line 52
--Cannot insert the value NULL into column 'CellPhone', table 'AutoTracker2.history.CustomerHistory'; column does not allow nulls. UPDATE fails.
--The statement has been terminated.

-- Why this this happen?
select * from dbo.Customer;
select * from history.CustomerHistory;

-- Update the email address a couple of times.
update dbo.Customer
set EmailAddress = 'miguel.jones@corporation.com'
where CustomerId = 1;

update dbo.Customer
set EmailAddress = 'anonymous@untraceable.net'
where CustomerId = 1;

-- We now have some history for email address.
select * from dbo.Customer;
select * from history.CustomerHistory;

-- What happens if we drop the column?
alter table dbo.Customer
drop column EmailAddress;

-- All history is gone as well.
select * from dbo.Customer;
select * from history.CustomerHistory;



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
