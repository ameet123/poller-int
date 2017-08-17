package org.ameet;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.integration.config.EnableIntegration;

/**
 * Created by ameet.chaubal on 8/17/2017.
 */
@SpringBootApplication
@EnableIntegration
public class PollerApp {

    public static void main(String[] args) throws InterruptedException {
        new SpringApplicationBuilder(PollerApp.class).run(args);
    }
}
