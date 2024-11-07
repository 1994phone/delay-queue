package cn.orins;

import cn.orins.db.DbUtils;
import cn.orins.db.Job;
import cn.orins.vo.DelayJobReqVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * packageName cn.orins
 *
 * @author xzy
 * @className JobController
 * @date 2024/11/1
 * @description TODO
 */
@RestController
@RequestMapping("/job")
public class JobController {
    @Autowired
    private DelayJobHandler delayJobHandler;

    @GetMapping("/testCreateJob")
    public String testCreateJob() {

        for (int i = 0; i < 10000; i++) {
            DelayJobReqVO delayJobReqVO = new DelayJobReqVO();
            delayJobReqVO.setParams("你好" + i);
            delayJobReqVO.setDelayTime(5000L );
            delayJobReqVO.setBizType("A");
            delayJobHandler.createJob(delayJobReqVO);
        }

        return "ok";
    }

    /**
     * 创建延迟任务
     *
     * @param reqVO 延迟任务请求参数
     * @return 任务id
     */
    @PostMapping("/createJob")
    public String createJob(@RequestBody DelayJobReqVO reqVO) {
        return delayJobHandler.createJob(reqVO);
    }

    /**
     * 取消延迟任务
     *
     * @param jobId 任务id
     * @return
     */
    @GetMapping("/cancelJob")
    public Boolean cancelJob(String jobId) {
        return delayJobHandler.cancelJob(jobId);
    }

    /**
     * 查询延迟任务
     *
     * @param jobId 任务id
     * @return
     */
    @GetMapping("/queryJob")
    public Job queryJob(String jobId) {
        return DbUtils.queryJob(jobId);
    }

    /**
     * 创建业务类型和topic的关系
     *
     * @param bizType 业务类型
     * @param topic   主题
     * @return
     */
    @GetMapping("/createTopicRelation")
    public Boolean createTopicRelation(String bizType, String topic) {
        DbUtils.saveTopicRelations(bizType, topic);
        return true;
    }

}
