Simple Java CosmosDB for MongoDB API Consumer to Kafka Producer to any Kafka deployment for continuous migrations.

Download the repo, replace the values for your CosmosDB for MongoDB API connection URI, values for your Kafka brokers and deployment configurations

mvn clean install
mvn exec:java -Dexec.mainClass="com.example.App"

And you should see your data flowing to Kafka!
