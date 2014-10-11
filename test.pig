raw = LOAD 'hbase://schools5'
  USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('addr:state', '-limit 10')
  AS (state:chararray);

st = FOREACH raw GENERATE state;
dump st;
