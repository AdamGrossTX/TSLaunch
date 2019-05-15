SELECT
	MachineName as DeviceName,
	ProjectedUpgradeDate,
	ProjectedDaysRemaining,
	CASE 
		WHEN UserScheduledForcedUpgrade = 'True' THEN 'UserScheduledUpgrade' 
		WHEN UserPressed = 'Install Now' THEN 'UserStartedUpgrade'
		WHEN AutoUpgradeStarted = 'True' THEN 'DeadlineStartedUpgrade'
		ELSE ScheduleType
	END as ScheduleType
FROM
(
SELECT 
	MachineName,
	MAX(CASE 
		WHEN InsString3 Like '%User scheduled a forced upgrade at:%'
		THEN 'True' ELSE NULL
	END) OVER (PARTITION BY M.MachineName) AS UserScheduledForcedUpgrade,

	MIN(CASE 
		WHEN InsString3 Like '%User scheduled a forced upgrade at:%'
		THEN SUBSTRING(InsString3,CHARINDEX('at:',InsString3)+4,LEN(InsString3)-CHARINDEX('at:',InsString3)-5)
		ELSE DATEADD(day, 21, Time)
	END) OVER (PARTITION BY M.MachineName) AS ProjectedUpgradeDate,

	MIN(DATEDIFF(day, GetDate(), CASE 
		WHEN InsString3 Like '%User scheduled a forced upgrade at:%'
		THEN SUBSTRING(InsString3,CHARINDEX('at:',InsString3)+4,LEN(InsString3)-CHARINDEX('at:',InsString3)-5)
		ELSE DATEADD(day, 21, Time)
	END)) OVER (PARTITION BY M.MachineName) AS ProjectedDaysRemaining,
	
	MAX(CASE 
		WHEN InsString3 Like '%Sucessfully launched the upgrade TS.%' THEN 'True'
	END) OVER (PARTITION BY M.MachineName) AS LaunchedTS,
	
	MAX(CASE 
		WHEN InsString3 Like '%This upgrade has expired.%attempting Upgrade.%' THEN 'True'
	END) OVER (PARTITION BY M.MachineName) AS AutoUpgradeStarted,
	CASE 
		WHEN InsString3 Like '%scheduling new attempt in %' THEN 'ScheduleType'
		WHEN InsString3 Like '%scheduling new attempt.%' THEN 'ScheduleType'
		WHEN InsString3 Like '%User scheduled a forced upgrade at:%' THEN 'ScheduleType'
		WHEN InsString3 Like '%Found a forced scheduled upgrade (user choice), attempting to start TS.%' THEN 'ScheduleType'
		WHEN InsString3 Like '%Sucessfully launched the upgrade TS.%' THEN 'ScheduleType'
		WHEN InsString3 Like '%Remaining days left to upgrade this computer:%' THEN 'ScheduleType'
		WHEN InsString3 Like '%User pressed ''btRun%' THEN 'UserPressed'
		WHEN InsString3 Like '%User pressed ''btMinimize%' THEN 'UserPressed'
		WHEN InsString3 Like '%User pressed ''btSchedule%' THEN 'UserPressed'
		WHEN InsString3 Like '%User pressed hyperlink%' THEN 'UserPressed'
		WHEN InsString3 Like '%User pressed ''Install now%' THEN 'UserPressed'
		WHEN InsString3 Like '%User pressed ''Tray Icon%' THEN 'UserPressed'
		ELSE NULL
	END AS [Key],
	CASE 
		WHEN InsString3 Like '%scheduling new attempt in %' THEN 'AutoPostponed'
		WHEN InsString3 Like '%scheduling new attempt.%' THEN 'UserPostponed'
		WHEN InsString3 Like '%User scheduled a forced upgrade at:%' THEN 'UserScheduledForce'
		WHEN InsString3 Like '%Found a forced scheduled upgrade (user choice), attempting to start TS.%' THEN 'UserForceDeadlined'
		WHEN InsString3 Like '%Remaining days left to upgrade this computer:%' THEN 'DefaultSchedule'
		WHEN InsString3 Like '%This upgrade has expired.%attempting Upgrade.%' THEN 'True'
		WHEN InsString3 Like '%User pressed ''btRun%' THEN 'Run'
		WHEN InsString3 Like '%User pressed ''btMinimize%' THEN 'Minimize'
		WHEN InsString3 Like '%User pressed ''btSchedule%' THEN 'Schedule'
		WHEN InsString3 Like '%User pressed hyperlink%' THEN 'Hyperlink'
		WHEN InsString3 Like '%User pressed ''Install now%' THEN 'Install Now'
		WHEN InsString3 Like '%User pressed ''Tray Icon%' THEN 'Tray Icon'
	END AS [Value]
FROM 
	v_StatMsgWithInsStrings as M
WHERE 
	M.InsString1 Like '%TSLaunchLog' 
AND MachineName in (select name0 from v_r_system)
) AS Source
PIVOT
(
MIN([Value])
FOR [Key] IN
(
	ScheduleType,
	UserPressed
)
) as PivotOut

ORDER BY MachineName