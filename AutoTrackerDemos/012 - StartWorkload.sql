if not exists (select * from dbo.Customer)
	exec utility.spGenerateRandomCustomers 10000;
if not exists (select * from dbo.Dealer)
	exec utility.spGenerateRandomDealers 1000;
if not exists (select * from dbo.Automobile)
	exec utility.spCreateRandomAutomobiles 10000;		
	
go
set nocount on;
go
while 0 = 0
begin
	exec utility.spExecuteRandomWorkloadAction;
	waitfor delay '0:00:00.250';
end
