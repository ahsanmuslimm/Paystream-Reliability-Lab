package com.paystream.notifier.config;

import com.paystream.common.AccountPool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NotifierConfig {

    @Bean
    public AccountPool accountPool(@Value("${paystream.account-pool-size:200}") int size) {
        return new AccountPool(size);
    }
}
