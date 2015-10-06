# Jar has been configured to get data from the broker under the user_input topic.The producer for this topic is defined in flask, based on when a user takes an action for a particular restaurant 
cd /usr/local/hadoop/etc/hadoop/
hadoop jar camus-example-0.1.0-SNAPSHOT-shaded.jar com.linkedin.camus.etl.kafka.CamusJob -P /usr/local/camus/camus-example/src/main/resources/camus.properties 
