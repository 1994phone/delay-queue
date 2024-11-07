package cn.orins.db;

import cn.orins.enums.JobStatusEnum;
import cn.orins.exceptions.JobException;
import cn.orins.vo.DelayJobReqVO;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * packageName cn.orins.db
 *
 * @author xzy
 * @className DbUtils
 * @date 2024/10/29
 * @description todo 后期可以替换成数据库，考虑一下情况觉得有必要对任务进行持久化
 * 1. 假如到期了，准备往kafka中推送任务，发现kafka异常，获取推送kafka 失败 ，redis中的数据是没了，此时就需要有地方记录任务，
 * 2. 任务留痕，所有任务都存起来，并且可溯
 * 3. 可以支持重试、获取旁路补推，在kafka恢复正常之后
 */
public class DbUtils {

    /**
     * 存储任务
     */
    private static final ConcurrentHashMap<String, Job> jobMap = new ConcurrentHashMap<>();

    /**
     * 存储业务类型和主题的关系
     */
    private static final ConcurrentHashMap<String, BizTypeAndTopicRelation> bizTypeAndTopicMap = new ConcurrentHashMap<>();

    public static void saveTopicRelations(String bizType, String topic) {
        if (bizType == null || bizType.isBlank()) {
            throw new JobException("业务类型缺失");
        }

        if (topic == null || topic.isBlank()) {
            throw new JobException("topic缺失");
        }

        if (bizTypeAndTopicMap.containsKey(bizType)) {
            throw new JobException("业务类型重复");
        }

        bizTypeAndTopicMap.put(bizType, new BizTypeAndTopicRelation(bizType, topic));
    }

    public static String getTopic(String bizType) {

        if (bizType == null || bizType.isBlank()) {
            throw new JobException("业务类型缺失");
        }

        BizTypeAndTopicRelation bizTypeAndTopicRelation = bizTypeAndTopicMap.get(bizType);
        if (bizTypeAndTopicRelation == null) {
            throw new JobException("不存在对应的关系");
        }

        return bizTypeAndTopicRelation.getTopic();
    }


    public static void saveJob(Job job) {
        if (jobMap.containsKey(job.getJobId())) {
            throw new JobException("任务ID重复");
        }
        jobMap.put(job.getJobId(), job);
    }

    /**
     * 改变任务状态
     *
     * @param jobId
     * @return
     */
    public static void changeJobStatus(String jobId, JobStatusEnum jobStatusEnum) {
        Job job = queryJob(jobId);
        job.setStatus(jobStatusEnum.getStatus());
        jobMap.put(jobId, job);
    }

    /**
     * 获取任务
     *
     * @param jobId 任务id
     * @return
     */
    public static Job queryJob(String jobId) {
        Job job = jobMap.get(jobId);
        if (job == null) {
            throw new JobException("任务不存在");
        }
        return job;
    }

    /**
     * 取消任务
     *
     * @param jobId
     * @return
     */
    public static void cancelJob(String jobId) {
        Job job = queryJob(jobId);
        job.setStatus(JobStatusEnum.CANCELED.getStatus());
        jobMap.put(jobId, job);
    }

    /**
     * 删除任务
     *
     * @param jobId
     * @return
     */
    public static void delJob(String jobId) {
        Job job = queryJob(jobId);
        job.setStatus(JobStatusEnum.DEL.getStatus());
        jobMap.put(jobId, job);
    }

}
