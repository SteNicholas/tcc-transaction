package org.mengyun.tcctransaction.recover;

import java.util.Set;

/**
 * 事务恢复策略配置
 */
public interface RecoverConfig {

    /**
     * 获取事务最多重试次数,表示一个事务最多尝试恢复次数,超过将不再自动恢复,需要人工干预,默认是30次
     *
     * @return
     */
    public int getMaxRetryCount();

    /**
     * 获取事务恢复间隔时间,表示一个事务日志当超过一定时间间隔后没有更新就会被认为是发生了异常,需要恢复,恢复Job将扫描超过这个时间间隔依旧没有更新的事务日志,并对这些事务进行恢复,时间单位是秒,默认是120秒
     *
     * @return
     */
    public int getRecoverDuration();

    /**
     * 获取恢复Job触发时间正则表达式,表示恢复Job触发间隔配置,默认是0 *|1 * * * ?
     *
     * @return
     */
    public String getCronExpression();

    /**
     * 获取延迟取消异常集合,表示系统发生设置的异常时候,主事务不立即rollback,而是由恢复Job来执行事务恢复.通常需要将超时异常设置为delayCancelExceptions,这样可以避免因为服务调用时发生了超时异常,主事务如果立刻rollback, 但是从事务还没执行完,从而造成主事务rollback失败
     *
     * @return
     */
    public Set<Class<? extends Exception>> getDelayCancelExceptions();

    public void setDelayCancelExceptions(Set<Class<? extends Exception>> delayRecoverExceptions);

    /**
     * 获取同步终结线程池大小
     *
     * @return
     */
    public int getAsyncTerminateThreadPoolSize();
}