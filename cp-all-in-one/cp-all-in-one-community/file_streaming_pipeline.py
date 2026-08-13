-- DROP CONNECTOR IF EXISTS csv_file_source;
CREATE SOURCE CONNECTOR IF NOT EXISTS csv_file_source WITH (
  'connector.class'               = 'com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector',
  'tasks.max'                     = '1',
  'topic'                         = 'US_hotel_reviews_csv',
  'input.path'                    = '/file-data',
  'finished.path'                 = '/file-data/finished',
  'error.path'                    = '/file-data/error',
  'input.file.pattern'            = '^Hotel_Reviews.*\.csv$',
  'halt.on.error'                 = 'false',
  'csv.first.row.as.header'       = 'true',
  'schema.generation.enabled'     = 'true',
  'key.converter'                 = 'org.apache.kafka.connect.storage.StringConverter',
  'value.converter'               = 'org.apache.kafka.connect.storage.StringConverter'
);