package cn.orins.buckets;

import cn.orins.db.DbUtils;
import cn.orins.enums.JobStatusEnum;
import cn.orins.exceptions.JobException;
import lombok.Data;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.LockSupport;

/**
 * packageName cn.orins
 *
 * @author xzy
 * @className DelayJobBucket
 * @date 2024/10/31
 * @description 存放等待过期的任务
 */
@Data
public class DelayJobBucket implements JobListener {

    Logger logger = LoggerFactory.getLogger(DelayJobBucket.class);

    /**
     * redisson 客户端
     */
    private RedissonClient redissonClient;

    /**
     * 桶名
     */
    private String bucketName;

    /**
     * 需要唤醒的线程, 保证监听线程和添加任务的线程 可见
     */
    private volatile Thread parkThread;

    /**
     * 准备就绪的桶
     */
    private ReadyJobBucket readyJobBucket;

    private volatile boolean start = Boolean.TRUE;

    public DelayJobBucket(RedissonClient redissonClient, ReadyJobBucket readyJobBucket, String bucketName) {
        this.redissonClient = redissonClient;
        this.bucketName = bucketName;
        this.readyJobBucket = readyJobBucket;
    }

    /**
     * 向桶里添加任务
     *
     * @param jobId      任务id
     * @param expireTime 过期时间
     */
    public synchronized void addDelayJob(String jobId, Long expireTime) {
        if (jobId == null) {
            throw new JobException("任务ID不能为空");
        }
        if (expireTime == null || expireTime < 100L) {
            throw new JobException("过期时间不能为空，也不支持小于100ms");
        }
        RScoredSortedSet<String> scoredSortedSet = redissonClient.getScoredSortedSet(bucketName);
        scoredSortedSet.add(expireTime, jobId);
        if (parkThread != null) {
            LockSupport.unpark(parkThread);
            parkThread = null;
        }
    }

    @Override
    public void run() {
        logger.info("{}已经启动.", this.getBucketName());
        while (start) {
            RScoredSortedSet<String> scoredSortedSet = redissonClient.getScoredSortedSet(bucketName);
            if (!scoredSortedSet.isExists()) {
                parkThread = Thread.currentThread();
                logger.warn("{}没有数据了,等待任务进来。", this.getBucketName());
                LockSupport.park();
                continue;
            }
            Double jobExpireTime = scoredSortedSet.firstScore();
            if (jobExpireTime != null && jobExpireTime <= System.currentTimeMillis()) {

                String jobId = scoredSortedSet.pollFirst();

                try {
                    //  将任务放入准备队列中去
                    readyJobBucket.addReadyJob(jobId);

                    //  然后更新这个db状态
                    DbUtils.changeJobStatus(jobId, JobStatusEnum.READY_WAIT);

                    logger.info("{}将任务{}存入就绪队列成功。", this.getBucketName(), jobId);
                } catch (Exception e) {
                    logger.error("{}将任务{}存入就绪队列失败。", this.getBucketName(), jobId, e);
                    // todo 告警
                    DbUtils.changeJobStatus(jobId, JobStatusEnum.ADD_READY_QUEUE_FAIL);
                }
            }
        }
    }

    /**
     * 停掉之后不再 处理到期任务
     */
    public void stop() {
        start = false;
        if (parkThread != null) {
            LockSupport.unpark(parkThread);
        }
        logger.info("{}已经停止.", this.getBucketName());
    }
}
