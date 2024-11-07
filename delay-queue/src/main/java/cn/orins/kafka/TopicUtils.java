package cn.orins.kafka;

import cn.orins.SpringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * packageName cn.orins
 *
 * @author xzy
 * @className TopicUtils
 * @date 2024/11/1
 * @description 给kafka推送消息
 */
public class TopicUtils {

    static Logger logger = LoggerFactory.getLogger(TopicUtils.class);
    private static KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 给kafka 推送消息
     *
     * @param topic   主题
     * @param message 消息
     */
    public static void send(String topic, String message, ListenableFutureCallback<SendResult<String, Object>> callback) {
        initKafkaTemplate();
        kafkaTemplate.send(topic, message).addCallback(callback);
    }

    private static void initKafkaTemplate() {
        if (kafkaTemplate == null) {
            kafkaTemplate = SpringUtils.getBean(KafkaTemplate.class);
        }
    }
}
