


package cn.orins;

import cn.orins.db.DbUtils;
import cn.orins.vo.DelayJobReqVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class Test {

    @Autowired
    private DelayJobHandler delayJobHandler; // 注入需要测试的服务

    @org.junit.jupiter.api.Test
    public void testServiceMethod() throws InterruptedException {

        DbUtils.saveTopicRelations("A", "ATopic");

        for (int i = 0; i < 100; i++) {
            DelayJobReqVO delayJobReqVO = new DelayJobReqVO();
            delayJobReqVO.setParams("你好");
            delayJobReqVO.setDelayTime(5000L);
            delayJobReqVO.setBizType("A");
            delayJobHandler.createJob(delayJobReqVO);
        }

        Thread.sleep(10000L);
    }
}
