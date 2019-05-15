SELECT 
	MachineName,
	RecordID,
	Time,
	MIN(RecordID) OVER (PARTITION BY MachineName) as FirstRecordID,
	MIN(Time) OVER (PARTITION BY MachineName) as FirstTime,
	MAX(RecordID) OVER (PARTITION BY MachineName) as MAXRecordID,
	MAX(Time) OVER (PARTITION BY MachineName) as MAXTime,
	TSLaunchStatus,
	Source,
	CollectionID,
	AbortOnProcess,
	ACConnected,
	AssessmentTest,
	BuildNumber,
	ContentAvailable,
	CustomPSScript,
	DiskSpace,
	NicConnected,
	PingServer,
	PPTRunning,
	RebootPending,
	WebEndpoint
FROM
(SELECT 
	M.RecordID,
	M.MachineName,
	M.Time,
	InsString5 as TSLaunchStatus,
	InsString1 as Source,
	InsString10 as CollectionID,
	KeyName,
	ResultValue
FROM 
	v_StatMsgWithInsStrings as M
	CROSS APPLY OPENJSON(InsString3 + InsString4)
WITH
	(KeyName nvarchar(50) '$.Key', ResultValue nvarchar(50) '$.Value')
WHERE
	M.InsString1 like 'TSLaunchStatus'
) AS Source
PIVOT
(
MIN(ResultValue)
FOR KeyName IN
(
	AbortOnProcess,
	ACConnected,
	AssessmentTest,
	BuildNumber,
	ContentAvailable,
	CustomPSScript,
	DiskSpace,
	NicConnected,
	PingServer,
	PPTRunning,
	RebootPending,
	WebEndpoint
)
) as Result

