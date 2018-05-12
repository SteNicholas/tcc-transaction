package org.mengyun.tcctransaction.spring.recover;

import org.mengyun.tcctransaction.OptimisticLockException;
import org.mengyun.tcctransaction.recover.RecoverConfig;

import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.Set;

/**
 * 默认事务恢复策略配置
 */
public class DefaultRecoverConfig implements RecoverConfig {

    public static final RecoverConfig INSTANCE = new DefaultRecoverConfig();

    /**
     * 事务最多重试次数默认为30次
     */
    private int maxRetryCount = 30;

    /**
     * 事务恢复间隔时间默认为120秒
     */
    private int recoverDuration = 120; //120 seconds

    /**
     * 恢复Job触发时间正则表达式默认为0 *|1 * * * ?
     */
    private String cronExpression = "0 */1 * * * ?";

    /**
     * 同步终结线程池大小默认为1024
     */
    private int asyncTerminateThreadPoolSize = 1024;

    /**
     * 延迟取消异常集合默认包括OptimisticLockException、SocketTimeoutException异常
     */
    private Set<Class<? extends Exception>> delayCancelExceptions = new HashSet<Class<? extends Exception>>();

    /**
     * Tcc事务切面中对乐观锁与socket超时异常不做回滚处理,只抛异常原因:
     * 不立即回滚,主要考虑是被调用服务方存在一直在正常执行的可能,只是执行的慢,导致了调用方超时,此时如果立即回滚,在被调用方执行Cancel操作的同时,被调用方的Try方法还在执行,甚至Cancel操作执行完了,try方法还没结束,这种情况下业务数据存在不一致的可能。目前解决办法是这类异常不立即回滚,而是由恢复Job执行回滚,恢复Job会在一段时间后再去调用该被调用方的Cancel方法,这个时间可在RecoverConfig中设置,默认120s
     */
    public DefaultRecoverConfig() {
        delayCancelExceptions.add(OptimisticLockException.class);
        //SocketTimeoutException:Try阶段本地参与者调用远程参与者(远程服务,例如Dubbo、Http服务),远程参与者Try的方法逻辑执行时间较长,超过Socket等待时长,发生 SocketTimeoutException异常;
        //如果立刻执行事务回滚,远程参与者Try的方法未执行完成,可能导致Cancel的方法实际未执行(Try的方法未执行完成,数据库事务[非TCC事务]未提交,Cancen的方法读取数据时发现未变更,导致方法实际未执行,
        //最终Try的方法执行完后,提交数据库事务[非TCC事务],较为极端),最终引起数据不一致.在事务恢复时,会对这种情况的事务进行取消回滚,如果此时远程参与者的 Try的方法还未结束,还是可能发生数据不一致
        delayCancelExceptions.add(SocketTimeoutException.class);
    }

    @Override
    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    @Override
    public int getRecoverDuration() {
        return recoverDuration;
    }

    @Override
    public String getCronExpression() {
        return cronExpression;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    public void setRecoverDuration(int recoverDuration) {
        this.recoverDuration = recoverDuration;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    @Override
    public void setDelayCancelExceptions(Set<Class<? extends Exception>> delayCancelExceptions) {
        this.delayCancelExceptions.addAll(delayCancelExceptions);
    }

    @Override
    public Set<Class<? extends Exception>> getDelayCancelExceptions() {
        return this.delayCancelExceptions;
    }

    public int getAsyncTerminateThreadPoolSize() {
        return asyncTerminateThreadPoolSize;
    }

    public void setAsyncTerminateThreadPoolSize(int asyncTerminateThreadPoolSize) {
        this.asyncTerminateThreadPoolSize = asyncTerminateThreadPoolSize;
    }
}