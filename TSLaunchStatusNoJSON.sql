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
(
SELECT 
	RecordID,
	MachineName,
	Time,
	InsString5 TSLaunchStatus,
	InsString1 as Source,
	InsString10 as CollectionID,
	SUBSTRING(j.value,1,CHARINDEX(':',j.value)-1) as KeyName,
	SUBSTRING(j.value,CHARINDEX(':',j.value)+1,LEN(j.value)-CHARINDEX(':',j.value)) as [Value]
FROM 
	v_StatMsgWithInsStrings as M
	CROSS APPLY (SELECT REPLACE(InsString3 + InsString4,'[','') AS value) a
	CROSS APPLY (SELECT REPLACE(a.value,']','') AS value) b
	CROSS APPLY (SELECT REPLACE(b.value,'{"Key":"','') AS value) c
	CROSS APPLY (SELECT REPLACE(c.value,'"},','|') AS value) d
	CROSS APPLY (SELECT REPLACE(d.value,'"Value":"','') AS value) e
	CROSS APPLY (SELECT REPLACE(e.value,'"','') AS value) f
	CROSS APPLY (SELECT REPLACE(f.value,',',':') AS value) g
	CROSS APPLY (SELECT REPLACE(g.value,'N\/A','') AS value) h
	CROSS APPLY (SELECT REPLACE(h.value,'}','') AS value) i
	CROSS APPLY	STRING_SPLIT(i.value,'|') j

WHERE
	M.InsString1 like 'TSLaunchStatus'
) AS Source
PIVOT
(
MIN([Value])
FOR [KeyName] IN
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
) as PivotOut