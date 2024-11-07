package cn.orins.buckets;

import cn.orins.exceptions.JobException;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * packageName cn.orins
 *
 * @author xzy
 * @className DelayJobBuckets
 * @date 2024/10/31
 * @description 桶的集合，用来管理桶
 */
public class DelayJobBuckets implements Closeable {

    Logger logger = LoggerFactory.getLogger(DelayJobBuckets.class);

    /**
     * 默认初始化10个，支持伸缩
     */
    private final List<DelayJobBucket> delayJobBucketList;

    /**
     * 桶名
     */
    private final ConcurrentHashMap<String, DelayJobBucket> bucketMap;

    /**
     * 桶集合的容量
     */
    private volatile int capacity;

    /**
     * 默认容量
     */
    private static final int DEFAULT_CAPACITY = 8;

    /**
     * 最大容量
     */
    private static final int MAX_CAPACITY = 32;

    /**
     * 监听桶的任务线程池
     */
    private final ThreadPoolExecutor executor;

    /**
     * redisson 客户端
     */
    private final RedissonClient redissonClient;

    private final ReentrantLock lock = new ReentrantLock();

    private final String BUCKET_NAME_PREFIX = "delay-job-";

    /**
     * 就绪桶
     */
    private final ReadyJobBucket readyJobBucket;

    public DelayJobBuckets(RedissonClient redissonClient) {
        this(redissonClient, DEFAULT_CAPACITY);
    }

    public DelayJobBuckets(RedissonClient redissonClient, int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("容量比如大于等于1");
        }
        this.bucketMap = new ConcurrentHashMap<>();
        this.redissonClient = redissonClient;
        this.capacity = Math.min(capacity, MAX_CAPACITY);
        this.delayJobBucketList = new ArrayList<>(this.capacity);
        this.executor = new ThreadPoolExecutor(this.capacity, MAX_CAPACITY, 0, TimeUnit.SECONDS, new SynchronousQueue<>(),
                new ThreadFactory() {
                    private final AtomicInteger count = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "delay-job-worker-" + count.incrementAndGet());
                    }
                });
        this.readyJobBucket = new ReadyJobBucket(redissonClient);
        this.readyJobBucket.start();
        for (int i = 1; i <= this.capacity; i++) {
            String bucketName = BUCKET_NAME_PREFIX + i;
            DelayJobBucket delayJobBucket = new DelayJobBucket(this.redissonClient, readyJobBucket, bucketName);
            this.delayJobBucketList.add(delayJobBucket);
            bucketMap.put(bucketName, delayJobBucket);
            executor.execute(delayJobBucket);
        }
    }

    /**
     * 桶名格式必须要保持统一
     * "delay-job-xx" , 不然中途宕机，重启之后，redis的任务就丢失了
     */
    public void addBucket() {
        lock.lock();
        try {
            if (this.capacity >= MAX_CAPACITY) {
                throw new JobException("桶数量超过限制");
            }

            String bucketName = BUCKET_NAME_PREFIX + this.capacity + 1;
            if (this.bucketMap.containsKey(bucketName)) {
                throw new JobException("桶名重复");
            }

            DelayJobBucket delayJobBucket = new DelayJobBucket(redissonClient, readyJobBucket, bucketName);
            this.delayJobBucketList.add(delayJobBucket);
            this.bucketMap.put(bucketName, delayJobBucket);
            this.executor.execute(delayJobBucket);
            this.capacity++;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 移除指定的桶
     * 慎用!!!!必须要等本桶里面没有待消费的数据才能调用，不然这个桶里面的数据会丢失
     *
     * @param bucketName 桶名
     */
    public void removeBucket(String bucketName) {
        lock.lock();
        try {
            DelayJobBucket delayJobBucket = this.bucketMap.get(bucketName);
            if (delayJobBucket != null) {
                this.bucketMap.remove(bucketName);
                this.delayJobBucketList.remove(delayJobBucket);
                delayJobBucket.stop();
                this.capacity--;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        logger.warn("服务正在崩溃，快速处理！！！！, 桶的数量是：{}", this.capacity);
        this.delayJobBucketList.forEach(DelayJobBucket::stop);
        this.readyJobBucket.stop();
        this.executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 选择适合的桶
     *
     * @param jobId 任务id
     * @return 桶
     */
    public DelayJobBucket chooseBucket(String jobId) {
        lock.lock();
        try {
            if (this.capacity == 0) {
                throw new JobException("没有可用的桶");
            }

            int hash = hash(jobId);
            int index = hash & (this.capacity - 1);
            return this.delayJobBucketList.get(index);
        } finally {
            lock.unlock();
        }
    }

    private int hash(String key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

}
