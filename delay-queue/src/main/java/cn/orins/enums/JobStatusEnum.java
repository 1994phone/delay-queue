package cn.orins.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

/**
 * packageName cn.orins.topic
 *
 * @author xzy
 * @className JobStatusEnum
 * @date 2024/10/29
 * @description 1. 初始状态；2. 已就绪；3. 执行成功 ； 4. 执行失败；5. 已补偿；6.已取消 ; 7.已删除
 */
@AllArgsConstructor
@Getter
public enum JobStatusEnum {

    INIT(1, "初始状态"),
    READY_WAIT(2, "已就绪-等待中"),
    SUCCESS(3, "执行成功"),
    FAIL(4, "执行失败"),
    //    COMPENSATED(5, "已补偿"),
    CANCELED(6, "已取消"),
    DEL(7, "已删除"),
    ADD_READY_QUEUE_FAIL(8, "存入就绪队列失败"),
    ;


    private final Integer status;

    private final String statusDesc;

    public static JobStatusEnum find(Integer status) {
        return Arrays.stream(values()).filter(x -> x.status.equals(status)).findFirst().orElse(null);
    }
}
