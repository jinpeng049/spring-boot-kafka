package com.easy.study.producer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaApplicationTests {

    @Autowired
    private Sender sender;

    @Test
    public void testReceive() throws Exception {
        long current = System.currentTimeMillis();
        for (int i = 0; i < 50000000; i++) {
            sender.send("helloWorld", "Hello Spring Kafka!" + i);
        }
        System.err.println((System.currentTimeMillis() - current) + "ms");
    }
}
