package cn.orins;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * packageName cn.orins
 *
 * @author xzy
 * @className RedisApp
 * @date 2024/10/22
 * @description TODO
 */
@SpringBootApplication
public class DelayQueueApp {

    public static void main(String[] args) {
        SpringApplication.run(DelayQueueApp.class);
    }

    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://localhost:6379");
        config.setCodec(new JsonJacksonCodec());

        return Redisson.create(config);
    }
}
