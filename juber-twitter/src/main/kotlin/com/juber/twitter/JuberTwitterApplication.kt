package com.juber.twitter

import com.fasterxml.jackson.databind.ObjectMapper
import com.juber.kafka.MessageSerializer
import com.juber.model.Message
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import twitter4j.*
import twitter4j.conf.ConfigurationBuilder
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

@SpringBootApplication
class JuberTwitterApplication(@Autowired val kafkaProducer: KafkaProducer<String, String>,
                              @Autowired val twitterStream: TwitterStream,
                              @Autowired val kafkaClient: KafkaClient) : CommandLineRunner {

    override fun run(vararg args: String) {
        Test.register(twitterStream, kafkaProducer)
        kafkaClient.addListener { msg ->
            val lng = msg.lngLat.lng
            val lat = msg.lngLat.lat

            twitterStream.cleanUp()
            twitterStream.filter(FilterQuery().locations(doubleArrayOf(lat, lng)))
        }
    }
}

fun main(args: Array<String>) {
    System.setProperty("server.port", "8087")
    SpringApplication.run(JuberTwitterApplication::class.java, *args)
}

@Component
class KafkaClient {
    private val executorService: ExecutorService = Executors.newSingleThreadExecutor()
    private val listeners: MutableList<Listener> = CopyOnWriteArrayList()
    private val kafkaConsumer: KafkaConsumer<String, Message>

    private val shutdown = AtomicBoolean()

    constructor(@Autowired kafkaConsumer: KafkaConsumer<String, Message>, mapper: ObjectMapper) {
        this.kafkaConsumer = kafkaConsumer

        executorService.submit {
            kafkaConsumer.subscribe(listOf("driver"))

            while (!shutdown.get()) {
                try {
                    kafkaConsumer.poll(10).map { it.value() }.forEach(this::notifyListeners)
                } catch(e: Exception) {
                    e.printStackTrace()
                }
            }

            kafkaConsumer.unsubscribe()
            kafkaConsumer.close()
        }
    }

    fun addListener(listener: Listener) {
        listeners.add(listener)
    }

    fun shutdown() = shutdown.set(true)

    private fun notifyListeners(message: Message) = listeners.forEach { it(message) }
}

typealias Listener = (message: Message) -> Unit

@Configuration
class Configuration {
    @Bean
    fun twitter_configuration() = ConfigurationBuilder()
            .setDebugEnabled(true)
            .setOAuthConsumerKey(System.getProperty("twitter.consumer.key"))
            .setOAuthConsumerSecret(System.getProperty("twitter.consumer.secret"))
            .setOAuthAccessToken(System.getProperty("twitter.access.token"))
            .setOAuthAccessTokenSecret(System.getProperty("twitter.access.key"))
            .build()

    @Bean
    fun twitter(configuration: twitter4j.conf.Configuration): TwitterStream = TwitterStreamFactory(configuration).instance

    @Bean
    fun serializer() = ObjectMapper()

    @Bean
    fun kafka_producer(): KafkaProducer<String, String> {
        val properties = Properties().apply {
            put("bootstrap.servers", "localhost:9092")
            put("group.id", "group-${Math.random()}")
            put("key.serializer", StringSerializer::class.java.canonicalName)
            put("key.deserializer", StringDeserializer::class.java.canonicalName)
            put("value.serializer", StringSerializer::class.java.canonicalName)
            put("value.deserializer", StringDeserializer::class.java.canonicalName)
            put("request.required.acks", "1")
        }
        return KafkaProducer(properties)
    }

    @Bean
    fun kafka_consumer(): KafkaConsumer<String, Message> {
        val properties = Properties().apply {
            put("bootstrap.servers", "localhost:9092")
            put("group.id", "group-${Math.random()}")
            put("key.serializer", StringSerializer::class.java.canonicalName)
            put("key.deserializer", StringDeserializer::class.java.canonicalName)
            put("value.serializer", MessageSerializer::class.java.canonicalName)
            put("value.deserializer", MessageSerializer::class.java.canonicalName)
            put("request.required.acks", "1")
        }
        return KafkaConsumer(properties)
    }
}
