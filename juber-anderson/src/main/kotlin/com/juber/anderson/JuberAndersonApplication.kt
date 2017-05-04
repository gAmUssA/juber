package com.juber.anderson

import com.fasterxml.jackson.databind.ObjectMapper
import com.juber.model.Message
import com.juber.kafka.MessageSerializer
import com.juber.kafka.SimplePartitioner
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.config.annotation.EnableWebSocket
import org.springframework.web.socket.config.annotation.WebSocketConfigurer
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry
import org.springframework.web.socket.handler.TextWebSocketHandler
import java.util.*

@SpringBootApplication
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
    fun kafka_client(): KafkaProducer<String, Message> {
        val properties = Properties().apply {
            put("metadata.broker.list", "localhost:9092")
            put("key.serializer", StringSerializer::class.java.canonicalName)
            put("value.serializer", MessageSerializer::class.java.canonicalName)
            //put("partitioner.class", SimplePartitioner::class.java.canonicalName)
            put("request.required.acks", "1")
        }

        return KafkaProducer(properties)
    }
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

@Component
class WebSocketHandler(@Autowired val serializer: ObjectMapper,
                       @Autowired val kafkaProducer: KafkaProducer<String, Message>) : TextWebSocketHandler() {

    override fun handleTextMessage(session: WebSocketSession, textMessage: TextMessage) {
        val payload = textMessage.payload
        val message = serializer.readValue(payload, Message::class.java)

        val uri = session.uri
        if (uri.path.contains("rider-ws")) {
            println("Rider-WS: " + payload)
            kafkaProducer.send(ProducerRecord<String, Message>("RIDER", message.timestamp.toString(), message))

        } else {
            println("Driver-WS: " + payload)
            kafkaProducer.send(ProducerRecord<String, Message>("DRIVER", message.timestamp.toString(), message))
        }
    }
}

