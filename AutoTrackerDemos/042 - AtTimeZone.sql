-----------------------------------------------------------------------------------------------------------------------
-- 042 - AtTimeZone.sql
-- Version 1.0.0
-- Look for the most recent version of this script at www.tf3604.com/temporal.
-- MIT License.  See the bottom of this file for details.
-----------------------------------------------------------------------------------------------------------------------

-- SQL now gives us time zone support.
-- Note that there is no 'Central Daylight Time' (always use 'Standard')
select sysdatetime() at time zone 'Central Standard Time' at time zone 'UTC';
select sysdatetimeoffset() at time zone 'UTC';
select sysutcdatetime();

-- What time is it in India right now?
select sysdatetimeoffset() at time zone 'India Standard Time';

-- Input must be a smalldatetime, datetime, datetime2 or datetimeoffset.
-- A string (that is a valid datetime) will fail.
select '2017-06-10 10:10:00' at time zone 'Central Standard Time' at time zone 'UTC';
--Msg 8116, Level 16, State 1, Line 8
--Argument data type varchar is invalid for argument 1 of AT TIME ZONE function.

-- Cast it to a valid data type.
select cast('2017-06-10 10:10:00' as datetime2) at time zone 'Central Standard Time' at time zone 'UTC';

-- We can see the valid times zone.
select * from sys.time_zone_info;



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
