CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_landing` (
  `user` string,
  `timestamp` bigint,
  `x` double,
  `y` double,
  `z` double
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-damiano-d609/accelerometer/landing/'
TBLPROPERTIES (
  'classification'='json'
);