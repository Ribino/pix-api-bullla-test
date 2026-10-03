package com.bullla.pix.partner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class PixPartnerMockApplication {

    public static void main(String[] args) {
        SpringApplication.run(PixPartnerMockApplication.class, args);
    }
}
