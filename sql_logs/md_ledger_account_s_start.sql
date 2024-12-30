INSERT INTO
	LOGS.LOGS_DS (
		ETL_TABLE,
		DATE_START,
		OPERATION_STATUS
	)
VALUES
	(
		'md_ledger_account_s',
		NOW()::TIME,
		6
	);
