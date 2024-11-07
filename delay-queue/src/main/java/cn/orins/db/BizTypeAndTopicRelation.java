package cn.orins.db;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 业务类型和主题的关系
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
public class BizTypeAndTopicRelation {

    /**
     * 类型
     */
    private String bizType;

    /**
     * topic
     */
    private String topic;

}
