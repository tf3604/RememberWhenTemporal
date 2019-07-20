-- Restore to SQL2016 instance.

/*

restore database bkhUtility
from disk = 'C:\data\Repos\RememberWhenTemporal\bkhUtility.bak'
with replace;

*/

-- Restore to EXPR2016SP1 instance

restore database bkhUtility
from disk = 'C:\data\Repos\RememberWhenTemporal\bkhUtility.bak'
with move 'AutoTracker' to 'C:\data\Express2016SP1\data\bkhUtility.mdf',
move 'AutoTracker_log' to 'C:\data\Express2016SP1\log\bkhUtility.ldf',
replace;

