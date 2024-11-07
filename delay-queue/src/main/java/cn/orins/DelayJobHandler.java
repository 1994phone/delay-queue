package cn.orins;

import cn.orins.buckets.DelayJobBucket;
import cn.orins.buckets.DelayJobBuckets;
import cn.orins.db.DbUtils;
import cn.orins.db.Job;
import cn.orins.enums.JobStatusEnum;
import cn.orins.exceptions.JobException;
import cn.orins.vo.DelayJobReqVO;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.UUID;

/**
 * packageName cn.orins
 *
 * @author xzy
 * @className DelayJobHandler
 * @date 2024/10/31
 * @description 接受任务的处理器
 */
@Service
public class DelayJobHandler implements DisposableBean, EnvironmentAware, InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(DelayJobHandler.class);

    /**
     * 最小的延迟时间为200毫秒
     */
    private final Long MIN_DELAY_TIME = 200L;

    private DelayJobBuckets delayJobBuckets;

    private Environment environment;

    @Autowired
    private RedissonClient redissonClient;

    /**
     * 创建延迟任务 需要枷锁，避免重复创建（会导致下游服务会重复执行），暂时用线程锁，其实分布式锁更好
     *
     * @param reqVO
     * @return 任务ID
     */
    public String createJob(DelayJobReqVO reqVO) {
        synchronized (this) {
            Job job = this.convertToJob(reqVO);

            DelayJobBucket delayJobBucket = delayJobBuckets.chooseBucket(job.getJobId());
            delayJobBucket.addDelayJob(job.getJobId(), job.getExpireTime());

            // 添加失败 就直接停止了
            DbUtils.saveJob(job);
            logger.info("任务创建成功。{},{}", reqVO, job.getJobId());
            return job.getJobId();
        }
    }

    /**
     * 取消延迟任务，需要枷锁，避免重复取消，暂时用线程锁，其实分布式锁更好
     * 1.未就绪的任务可以取消
     *
     * @param jobId 任务id
     * @return
     */
    public Boolean cancelJob(String jobId) {
        synchronized (this) {
            Job job = DbUtils.queryJob(jobId);
            JobStatusEnum jobStatusEnum = JobStatusEnum.find(job.getStatus());
            if (jobStatusEnum != JobStatusEnum.INIT) {
                logger.error("任务无法取消，不是初始状态了。{},{}", jobId, jobStatusEnum);
                return false;
            }
            DbUtils.cancelJob(jobId);
            logger.info("任务取消成功。{}", jobId);
            return true;
        }
    }

    private Job convertToJob(DelayJobReqVO jobReqVO) {
        if (jobReqVO == null
                || jobReqVO.getBizType() == null
                || jobReqVO.getBizType().isBlank()
                || jobReqVO.getDelayTime() == null
                || jobReqVO.getDelayTime() <= MIN_DELAY_TIME
                || jobReqVO.getParams() == null
                || jobReqVO.getParams().isBlank()) {
            throw new JobException("参数有误");
        }

        String topic = DbUtils.getTopic(jobReqVO.getBizType());
        if (topic == null) {
            throw new JobException("不存在对应的topic");
        }

        Job job = new Job();
        job.setJobId(UUID.randomUUID().toString());
        job.setBizType(jobReqVO.getBizType());
        job.setParams(jobReqVO.getParams());
        job.setDelayTime(jobReqVO.getDelayTime());
        job.setCreateTime(new Date());
        job.setExpireTime(System.currentTimeMillis() + jobReqVO.getDelayTime());
        job.setStatus(JobStatusEnum.INIT.getStatus());
        return job;
    }

    @Override
    public void destroy() throws Exception {
        delayJobBuckets.close();
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // 项目启动的时候 指定了桶的数据的话，就使用，不然就默认
        String capacity = this.environment.getProperty("delay.job.capacity");
        if (capacity != null && !capacity.isBlank()) {
            this.delayJobBuckets = new DelayJobBuckets(redissonClient, Integer.parseInt(capacity));
        } else {
            this.delayJobBuckets = new DelayJobBuckets(redissonClient);
        }
    }
}
