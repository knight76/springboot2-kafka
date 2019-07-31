package com.google.knight76.service;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class KafkaConsumer {

	@Value("${app.group.id}")
	private String groupId;

	// for schemaless json format
	@KafkaListener(topics = "${app.topic}", groupId = "${app.group.id}", containerFactory = "kafkaListenerContainerFactory")
	public void consume(String message) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Map<Object, Object> map = mapper.readValue(message, new TypeReference<Map<Object, Object>>(){});
		System.out.println(map);
	}
}
