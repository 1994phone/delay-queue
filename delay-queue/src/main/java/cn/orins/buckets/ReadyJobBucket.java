package cn.orins.buckets;

import cn.orins.db.DbUtils;
import cn.orins.db.Job;
import cn.orins.enums.JobStatusEnum;
import cn.orins.kafka.TopicUtils;
import lombok.Data;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * packageName cn.orins
 *
 * @author xzy
 * @className ReadyJobBucket
 * @date 2024/11/1
 * @description 到达延迟时间的任务 存放桶
 */
@Data
public class ReadyJobBucket implements JobListener {

    Logger logger = LoggerFactory.getLogger(ReadyJobBucket.class);

    /**
     * 桶名
     */
    private final String bucketName;

    /**
     * redisson客户端
     */
    private final RedissonClient redissonClient;

    /**
     * 等待唤醒的线程
     */
    private volatile Thread parkThread;

    /**
     * 启动标识
     */
    private volatile boolean start;

    /**
     * 借用IO多路模型，一个主线程去监听任务，多个工作线程去处理任务
     */
    private final ThreadPoolExecutor workerExecutor;

    private final Integer MAX_POOL_SIZE = 30;

    public ReadyJobBucket(String bucketName, RedissonClient redissonClient) {
        this.bucketName = bucketName;
        this.redissonClient = redissonClient;
        // 目前存在丢任务的风险
        this.workerExecutor = new ThreadPoolExecutor(5, MAX_POOL_SIZE, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000),
                new ThreadFactory() {
                    final AtomicInteger count = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "ready-delay-job-worker-" + count.incrementAndGet());
                    }
                });
    }

    public ReadyJobBucket(RedissonClient redissonClient) {
        this("ready-delay-job", redissonClient);
    }

    /**
     * 添加已准备好的任务Id
     *
     * @param jobId 任务Id
     */
    public void addReadyJob(String jobId) {
        // 使用不重复队列
        RList<String> list = redissonClient.getList(bucketName);
        list.add(jobId);
        if (parkThread != null) {
            LockSupport.unpark(parkThread);
            parkThread = null;
        }
    }

    @Override
    public void run() {
        while (start) {
            RBlockingQueue<String> blockingQueue = redissonClient.getBlockingQueue(bucketName);
            if (!blockingQueue.isExists()) {
                parkThread = Thread.currentThread();
                LockSupport.park();
                continue;
            }

            String jobId = blockingQueue.poll();
            if (jobId == null || jobId.isBlank()) {
                continue;
            }

            try {
                // 工作线程去处理
                workerExecutor.execute(() -> processJob(jobId));
            } catch (RejectedExecutionException e) {
                // 任务被拒绝了，说明超过线程池的处理能力，试运行阶段可以看看这个出险的次数，后续根据这个来调整线程池
                logger.error("任务被线程池拒绝了。{}", jobId, e);
                // 重新加入到就绪队列
                this.addReadyJob(jobId);
            }
        }
    }

    private void processJob(String jobId) {
        try {
            // 获取任务
            Job job = DbUtils.queryJob(jobId);

            // 校验任务状态
            JobStatusEnum jobStatusEnum = JobStatusEnum.find(job.getStatus());
            if (jobStatusEnum == JobStatusEnum.CANCELED
                    || jobStatusEnum == JobStatusEnum.DEL) {
                logger.warn("任务状态有误，停止执行。id:{},状态:{}", jobId, jobStatusEnum.getStatusDesc());
                return;
            }

            // 获取到当前任务绑定的主题
            String topic = DbUtils.getTopic(job.getBizType());

            // 推送消息
            TopicUtils.send(topic, job.getParams(), new ListenableFutureCallback<SendResult<String, Object>>() {
                @Override
                public void onFailure(Throwable ex) {
                    logger.error("发送消息失败，topic:{},message:{},exception:{}", topic, job.getParams(), ex.getMessage());
                    // todo 告警
                    DbUtils.changeJobStatus(jobId, JobStatusEnum.FAIL);
                }

                @Override
                public void onSuccess(SendResult<String, Object> result) {
                    logger.info("发送消息成功.topic:{},partition:{},offset:{}", result.getRecordMetadata().topic(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    DbUtils.changeJobStatus(jobId, JobStatusEnum.SUCCESS);
                }
            });
        } catch (Exception e) {
            logger.error("执行就绪任务失败。{}", jobId, e);
            // todo 告警
            DbUtils.changeJobStatus(jobId, JobStatusEnum.FAIL);
        }
    }

    /**
     * 停掉之后不再 处理就绪任务
     */
    public void stop() {
        start = false;
        if (parkThread != null) {
            LockSupport.unpark(parkThread);
        }
        workerExecutor.shutdown();  // 停止线程池
        try {
            if (!workerExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                workerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerExecutor.shutdownNow();
        }
    }

    public synchronized void start() {
        if (start) {
            logger.warn("ReadyJobBucket已经启动了。不用重复启动。");
            return;
        }
        start = true;
        new Thread(this).start();
        logger.info("ReadyJobBucket已经启动。");
    }
}
