package cn.orins.vo;

import lombok.Data;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * packageName cn.orins.vo
 *
 * @author xzy
 * @className Job
 * @date 2024/10/29
 * @description 延迟任务的请求参数
 */
@Data
public class DelayJobReqVO implements Serializable {

    /**
     * 业务标识
     */
    private String bizType;

    /**
     * 任务参数
     */
    private String params;

    /**
     * 延迟时间 毫秒
     */
    private Long delayTime;

}
