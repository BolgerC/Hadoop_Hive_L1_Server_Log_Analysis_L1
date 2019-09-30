DROP TABLE IF EXISTS server_log

CREATE TABLE IF NOT EXISTS server_log (
	ip_address STRING,
	client_identd STRING,
	client_userid STRING,
	time STRING,
	http_method STRING,
	resource_path STRING,
	protocol STRING,
	status_code INT,
	size INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^]*) ([^]*) ([^]*) (-|\\[^\\]*\\]) (\"[^ \"]*) ([^ \"]*) ([^ \"]*\") (-|[0-9]*) (-|[0-9]*)"
)
STORED AS TEXTFILE;