package com.juber.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.juber.model.Message
import groovy.transform.CompileStatic
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

@CompileStatic
class MessageSerializer implements Serializer, Deserializer<Message> {

    @Override
    void configure(Map configs, boolean isKey) {
    }

    @Override
    byte[] serialize(String topic, Object data) {
        byte[] retVal = null
        ObjectMapper objectMapper = new ObjectMapper()
        try {
            retVal = objectMapper.writeValueAsString(data).getBytes()
        } catch (Exception e) {
            e.printStackTrace()
        }
        return retVal
    }

    @Override
    Message deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper()
        Message message = null
        try {
            message = mapper.readValue(data, Message.class)
        } catch (Exception e) {
            e.printStackTrace()
        }
        return message
    }

    @Override
    void close() {}
}
