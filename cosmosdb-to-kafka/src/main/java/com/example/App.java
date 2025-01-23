package com.example;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class App {

    // CosmosDB for MongoDB API connection string
    static ConnectionString theconnectionstring = new ConnectionString("mongodb://adrianmongoapi:aPjh8ZNDMR6MVXCONX38w434ksCsIdXrbBWbYxeF7j8GRArL7bDNmTrMo9WWwljc5iGJ0CS91al4ACDbutYOIA==@adrianmongoapi.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@adrianmongoapi@");
    private static final String DATABASE_NAME = "stressazuredb2";
    private static final String COLLECTION_NAME = "somecol2";

    // Kafka configuration
    private static final String KAFKA_BROKER = "PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092";
    private static final String KAFKA_TOPIC = "testreading";

    public static void main(String[] args) {
        // Configure MongoDB client (CosmosDB Mongo API)

        try (MongoClient mongoClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(theconnectionstring).build()
        )) {

            MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

            // Setup Kafka producer
            Properties kafkaProps = new Properties();
            kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<>(kafkaProps);
            try {
                // Query all documents from CosmosDB (using MongoDB syntax)
                FindIterable<Document> documents = collection.find();

                // Iterate over each document and send it to Kafka
                for (Document doc : documents) {
                    String jsonString = doc.toJson();
                    ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, jsonString);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.printf("Error sending record: %s%n", exception.getMessage());
                        } else {
                            System.out.printf("Record sent to partition %d with offset %d%n",
                                    metadata.partition(), metadata.offset());
                        }
                    });
                }
            } finally {
                // Close Kafka producer
                producer.close(Duration.ofSeconds(10));
            }

            System.out.println("Process completed successfully.");
        } catch (Exception e) {
            System.err.println("An error occurred while connecting to Cosmos DB or sending data to Kafka: ");
            e.printStackTrace();
        }
    }
}
