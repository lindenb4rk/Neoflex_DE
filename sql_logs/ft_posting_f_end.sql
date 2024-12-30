UPDATE LOGS.LOGS_DS
SET
	DATE_END = NOW()::TIME,
	OPERATION_STATUS = 0,
	TIME_ETL = NOW()::TIME - DATE_START
WHERE
	OPERATION_STATUS = 2;