package com.juber.anderson

import com.fasterxml.jackson.databind.ObjectMapper
import com.hazelcast.jet.Jet
import com.hazelcast.jet.JetInstance
import com.juber.hasselhoff.jet.StreamHelper
import com.juber.kafka.MessageSerializer
import com.juber.model.Message
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.config.annotation.EnableWebSocket
import org.springframework.web.socket.config.annotation.WebSocketConfigurer
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry
import org.springframework.web.socket.handler.TextWebSocketHandler
import java.util.*
import java.util.function.Supplier

@SpringBootApplication(exclude = arrayOf(HazelcastAutoConfiguration::class))
class JuberAndersonApplication

fun main(args: Array<String>) {
    SpringApplication.run(JuberAndersonApplication::class.java, *args)
}

@Configuration
@EnableWebSocket
class WebSocketConfiguration(@Autowired val webSocketHandler: WebSocketHandler) : WebSocketConfigurer {
    override fun registerWebSocketHandlers(registry: WebSocketHandlerRegistry) {
        registry.addHandler(webSocketHandler, "rider-ws")
        registry.addHandler(webSocketHandler, "driver-ws")
    }
}

@Configuration
class Configuration {
    @Bean
    fun serializer() = ObjectMapper()

    @Bean
    @Qualifier("kafka")
    fun kafka_properties(): Properties = Properties().apply {
        put("bootstrap.servers", "localhost:9092")
        put("group.id", "group-${Math.random()}")
        put("key.serializer", StringSerializer::class.java.canonicalName)
        put("key.deserializer", StringDeserializer::class.java.canonicalName)
        put("value.serializer", MessageSerializer::class.java.canonicalName)
        put("value.deserializer", MessageSerializer::class.java.canonicalName)
        //put("partitioner.class", SimplePartitioner::class.java.canonicalName)
        put("request.required.acks", "1")
    }

    @Bean
    fun kafka_producer(@Qualifier("kafka") properties: Properties) = KafkaProducer<String, Message>(properties)

    @Bean
    fun kafka_consumer_supplier(@Qualifier("kafka") properties: Properties) =
            Supplier { KafkaConsumer<String, Message>(properties) }

    @Bean
    fun jet_client() = Jet.newJetClient()
}

@Controller
class ViewController {
    val accessToken = "pk.eyJ1Ijoibm9jdGFyaXVzIiwiYSI6ImNqMmE3MzU2djAwMGYzM24wZ3UwZHF1cTQifQ.prOT_aWKF16SitLgkFfq0w"

    @RequestMapping("/rider")
    fun rider_ui(model: Model): String {
        model.addAttribute("accesstoken", accessToken)
        model.addAttribute("wsurl", "ws://localhost:8080/rider-ws")
        return "rider"
    }

    @RequestMapping("/driver")
    fun driver_ui(model: Model): String {
        model.addAttribute("accesstoken", accessToken)
        model.addAttribute("wsurl", "ws://localhost:8080/driver-ws")
        return "driver"
    }
}

@RestController
class AdminController(@Autowired val jetInstance: JetInstance) {
    @GetMapping("/api/statistics", produces = arrayOf(MediaType.APPLICATION_JSON_UTF8_VALUE))
    fun statistics(): String {
        val avg = StreamHelper.avg(jetInstance)
        return "{ avg: $avg }"
    }
}

@Component
class WebSocketHandler(@Autowired val mapper: ObjectMapper,
                       @Autowired val kafkaProducer: KafkaProducer<String, Message>,
                       @Autowired val kafkaConsumerSupplier: Supplier<KafkaConsumer<String, Message>>) : TextWebSocketHandler() {

    override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
        val consumer = session.attributes.get("consumer")
        if (consumer is KafkaConsumer<*, *>) {
            consumer.unsubscribe()
            consumer.close()
        }
    }

    override fun handleTextMessage(session: WebSocketSession, textMessage: TextMessage) {
        try {
            val driver = session.uri.path.contains("driver-ws")
            val sink = if (driver) "driver" else "rider"
            val source = if (driver) "rider" else "driver"

            val kafkaConsumer: KafkaConsumer<String, Message> = session.createKafkaConsumer(source)

            val payload = textMessage.payload
            val message = mapper.readValue(payload, Message::class.java)

            kafkaProducer.send(ProducerRecord<String, Message>(sink, message.timestamp.toString(), message))

            kafkaConsumer.poll(10).map { mapper.writeValueAsString(it.value()) }.forEach { session.sendMessage(TextMessage(it)) }
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        }
    }

    fun WebSocketSession.createKafkaConsumer(topic: String): KafkaConsumer<String, Message> {
        val attributes = this.attributes
        return attributes.computeIfAbsent("consumer", { k ->
            kafkaConsumerSupplier.get().apply { subscribe(listOf(topic)) }
        }) as KafkaConsumer<String, Message>
    }
}
