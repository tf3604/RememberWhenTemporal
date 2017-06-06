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
