INSERT INTO
	LOGS.LOGS_DS (
		ETL_TABLE,
		DATE_START,
		OPERATION_STATUS
	)
VALUES
	(
		'ft_posting_f',
		NOW()::TIME,
		2
	);

DELETE FROM DS.FT_POSTING_F;