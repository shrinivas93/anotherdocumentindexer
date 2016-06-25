package com.shrinivas.documentindexer.launcher;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ApplicationLauncher {
	
	private static final Logger LOGGER = LogManager.getLogger(ApplicationLauncher.class);
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		LOGGER.info("Starting Application");
		new ClassPathXmlApplicationContext("classpath:applicationContext.xml");
		LOGGER.info("Spring Context Loaded");
	}
}
