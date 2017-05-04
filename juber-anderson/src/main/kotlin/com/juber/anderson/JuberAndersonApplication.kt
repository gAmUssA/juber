package com.juber.anderson

import com.fasterxml.jackson.databind.ObjectMapper
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
class WebSocketHandler(@Autowired val serializer: ObjectMapper) : TextWebSocketHandler() {
    override fun handleTextMessage(session: WebSocketSession, textMessage: TextMessage) {
        val payload = textMessage.payload
        val message = serializer.readValue(payload, Message::class.java)

        val uri = session.uri
        if (uri.path.contains("rider-ws")) {
            // TODO Kafka integration
            println("Rider-WS: " + payload)
        } else {
            // TODO Kafka integration
            println("Driver-WS: " + payload)
        }
    }
}

class Message {
    var driver: String? = null
    var lngLat: LngLat? = null
    var rider: String? = null
    var status: String? = null
}

class LngLat {

    var lat: Double? = null
    var lng: Double? = null
}
