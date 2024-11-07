package cn.orins.db;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

/**
 * 延迟任务
 */
@Getter
@Setter
@ToString
public class Job {

    /**
     * job唯一标识
     */
    private String jobId;

    /**
     * 类型
     */
    private String bizType;

    /**
     * 延迟时间，默认是毫秒
     */
    private Long delayTime;

    /**
     * 到期时间，默认是毫秒 当前时间 + delayTime
     */
    private Long expireTime;

    /**
     * 任务参数
     */
    private String params;

    /**
     * 任务状态：1. 等待中；2. 已就绪；3. 执行成功 ； 4. 执行失败；5. 已补偿；6.已取消 ; 7.已删除
     */
    private Integer status;

    /**
     * 创建时间
     */
    private Date createTime;

}
