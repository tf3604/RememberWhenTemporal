-----------------------------------------------------------------------------------------------------------------------
-- 013 - VerifyWorkload.sql
-- Version 1.0.5
-- Look for the most recent version of this script at www.tf3604.com/temporal.
-- MIT License.  See the bottom of this file for details.
-----------------------------------------------------------------------------------------------------------------------
use AutoTracker;
go

select * from history.OwnershipHistory;
select * from history.CustomerHistory;
select * from history.DealerHistory;
select * from history.AutomobileHistory;

select * from dbo.Ownership for system_time all
where OwnershipId = 2622
order by ValidFrom;

select *
from sys.partitions p
where p.object_id = object_id('history.OwnershipHistory');



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
