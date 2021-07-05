from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment, StreamTableEnvironment
from pyflink.table.descriptors import Schema, Kafka
from pyflink.table.expressions import col
from pyflink.table.udf import udf

# environment configuration
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(environment_settings=env_settings)
t_env.get_config().get_configuration().set_string('parallelism.default', '1')



# register akamai_log table and in table environment

source_ddl = f"""
          CREATE TABLE akamai_log (
          
          -- kafka keys
          clientIp string
          ,channel string
          ,statusCode int
          
          -- a few columns mapped to the Avro fields of the Kafka value
          ,clientIp STRING
          ,UA STRING
          ,statusCode INT
          ,totalBytes INT
          ,reqPath STRING
          ,reqHost STRING
          ,hyperfield STRING
          ,event_time TIMESTAMP(3) METADATA FROM 'timestamp'
                    
        ) WITH (
        
          'connector' = 'kafka',
          'topic' = 'akamai-cdn-avro',
          'properties.bootstrap.servers' = 'pkc-ldvj1.ap-southeast-2.aws.confluent.cloud:9092',
          'properties.security.protocol'= 'SASL_SSL',
          'properties.sasl.mechanisms' = 'PLAIN',
          'properties.sasl.username'= '6M4HYEWGYRUW7LIV',
          'properties.sasl.password'='kDuiUyy9AUI14hD5KdRLQRAte/qOmDD38/vcRrQcN8D4GEkMtBiblLcDXve7jbtL',
        
          -- Watch out: schema evolution in the context of a Kafka key is almost never backward nor
          -- forward compatible due to hash partitioning.
          'key.format' = 'avro-confluent',
          'key.avro-confluent.schema-registry.url' = 'https://psrc-lzvd0.ap-southeast-2.aws.confluent.cloud',
          'key.fields' = 'clientIp;channel;statusCode',
        
          -- In this example, we want the Avro types of both the Kafka key and value to contain the field 'id'
          -- => adding a prefix to the table column associated to the Kafka key field avoids clashes
          'key.fields-prefix' = 'kafka_key_',
        
          'value.format' = 'avro-confluent',
          'value.avro-confluent.schema-registry.url' = 'https://psrc-lzvd0.ap-southeast-2.aws.confluent.cloud',
          'value.fields-include' = 'EXCEPT_KEY',
           
          -- subjects have a default value since Flink 1.13, though can be overriden:
          'key.avro-confluent.schema-registry.subject' = 'akamai-cdn-avro-key',
          'value.avro-confluent.schema-registry.subject' = 'akamai-cdn-avro-value',
          'avro-confluent.basic-auth.user-info' = '6QNT4XLJTZU4YDWM',
          'avro-confluent.basic-auth.credentials-source' = '0nUTwQ95BqzziLOEpqGRPIL++U0LS/Y2JLa6yUjfYxtIxGTqx/Fn+PhHZpjVc3l4'
        )
        """
        
        
        
        
        
source = t_env.execute_sql(source_ddl)



res = t_env.execute_sql(f"""
                  SELECT * FROM akamai_log
                  """
                  )

with res.collect() as results:
   for result in results:
       print(result)    
    
    

# specify table program
#edgelogs = t_env.from_path('akamai_log')  

#edgelogs.group_by('clientIp').select(edgelogs.clientIp, edgelogs.statusCode.count.alias('cnt')).insert_into("mySink")

