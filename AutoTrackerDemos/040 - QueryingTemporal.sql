use AutoTracker2;
go

select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
from history.AutoModelHistory
where ModelId = 1;

-- Normally, we don't query the history table directly.  Rather, there are new SQL language features to help us out.
-- This is the "for system_time" clause, which comes in several varieties.
-- The most common is probably "as of"
-- Note: replace the timestamp below with a time between the ValidFrom and ValidTo from the history query above.
select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
from dbo.AutoModel for system_time as of '2017-06-06 19:58:02.6600107'
where ModelId = 1;

-- View the query plan from the previous query to show that it is accessing the history table even though it's
-- not being explicitly referenced in the query.

-- Other constructs allow us to specify a range of valid times.
-- The difference between these constructs is what endpoints are or are not included.
-- Note that multiple versions of a row can be returned.
-- BETWEEN start AND end
-- start < ValidTo AND end >= ValidFrom
select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
from dbo.AutoModel for system_time between '2017-06-06 19:58:02.6600107' and '2017-06-06 20:15:02.6600107'
where ModelId = 1;

-- FROM start TO end
-- start < ValidTo AND end > ValidFrom
select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
from dbo.AutoModel for system_time from '2017-06-06 19:58:02.6600107' to '2017-06-06 20:15:02.6600107'
where ModelId = 1;

-- CONTAINED IN (start, end)
-- start >= ValidTo AND end <= ValidFrom
-- Note that the record must be valid for the full period to be returned.
select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
from dbo.AutoModel for system_time contained in ('2017-06-06 19:58:02.6600107', '2017-06-06 20:15:02.6600107')
where ModelId = 1;

-- ALL
-- Return full history.
select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
from dbo.AutoModel for system_time all
where ModelId = 1;

-- We can even query the table at a point in time before the table was created:
select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
from dbo.AutoModel for system_time as of '1776-07-04'
where ModelId = 1;

-- Querying in the future is really about the same as querying the core table.
select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
from dbo.AutoModel for system_time as of '2099-12-31'
where ModelId = 1;

go
-- Note that arguments to "for system_time' can only be literals or variables.
-- This is OK.
declare @timeValue datetime2 = (select ValidFrom from dbo.Manufacturer where ManufacturerId = 1);

select mfg.*
from dbo.Manufacturer mfg
cross apply
(
	select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
	from dbo.AutoModel for system_time as of @timeValue
	where ModelId = 1
) mdl;
go

-- But this is not.
select mfg.*
from dbo.Manufacturer mfg
cross apply
(
	select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
	from dbo.AutoModel for system_time as of mfg.ValidFrom
	where ModelId = 1
) mdl;
--Msg 102, Level 15, State 1, Line 141
--Incorrect syntax near 'mfg'.
go

-- Nor is this.
select ModelId, ManufacturerId, ModelName, Description, ValidFrom, ValidTo
from dbo.AutoModel for system_time as of (select ValidFrom from dbo.Manufacturer where ManufacturerId = 1)
where ModelId = 1
--Msg 102, Level 15, State 1, Line 149
--Incorrect syntax near '('.
--Msg 156, Level 15, State 1, Line 150
--Incorrect syntax near the keyword 'where'.
go
