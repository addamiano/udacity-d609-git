CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_landing` (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` int
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-damiano-d609/step_trainer/landing/'
TBLPROPERTIES (
  'classification'='json'
);