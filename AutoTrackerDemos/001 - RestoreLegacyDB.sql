/*
restore database LegacyAutoTracker
from disk = 'C:\data\Repos\RememberWhenTemporal\LegacyAutoTracker.bak'
with replace;
*/

restore database LegacyAutoTracker
from disk = 'C:\data\Repos\RememberWhenTemporal\LegacyAutoTracker.bak'
with move 'LegacyAutoTracker' to 'C:\data\Express2016SP1\data\LegacyAutoTracker.mdf',
move 'LegacyAutoTracker_log' to 'C:\data\Express2016SP1\log\LegacyAutoTracker_log.ldf',
replace;
