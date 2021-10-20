package server.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.annotation.SessionScope;
import server.model.pojo.PersistentFields;

@Configuration
public class Config {
    @Bean("persistentFields")
    @SessionScope
    public PersistentFields getPersistenceFields() {
        return new PersistentFields();
    }
}
