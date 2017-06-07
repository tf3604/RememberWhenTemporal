-- SQL now gives us time zone support.
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
