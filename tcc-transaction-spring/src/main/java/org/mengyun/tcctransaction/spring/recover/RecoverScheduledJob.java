package org.mengyun.tcctransaction.spring.recover;

import org.mengyun.tcctransaction.SystemException;
import org.mengyun.tcctransaction.recover.TransactionRecovery;
import org.mengyun.tcctransaction.support.TransactionConfigurator;
import org.quartz.Scheduler;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.MethodInvokingJobDetailFactoryBean;

/**
 * 事务恢复定时任务,基于Quartz实现事务恢复任务调度
 * 事务重试通过乐观锁解决,只有一个节点能获得控制权来处理,其它节点不能处理则忽略
 */
public class RecoverScheduledJob {

    private TransactionRecovery transactionRecovery;

    private TransactionConfigurator transactionConfigurator;

    private Scheduler scheduler;

    public void init() {
        try {
            //配置事务恢复定时任务基础信息
            MethodInvokingJobDetailFactoryBean jobDetail = new MethodInvokingJobDetailFactoryBean();
            jobDetail.setTargetObject(transactionRecovery);
            jobDetail.setTargetMethod("startRecover");
            jobDetail.setName("transactionRecoveryJob");
            //禁止任务并发执行
            jobDetail.setConcurrent(false);
            jobDetail.afterPropertiesSet();

            //配置事务恢复定时任务触发器信息
            CronTriggerFactoryBean cronTrigger = new CronTriggerFactoryBean();
            cronTrigger.setBeanName("transactionRecoveryCronTrigger");
            cronTrigger.setCronExpression(transactionConfigurator.getRecoverConfig().getCronExpression());
            cronTrigger.setJobDetail(jobDetail.getObject());
            cronTrigger.afterPropertiesSet();

            //调度事务恢复任务
            scheduler.scheduleJob(jobDetail.getObject(), cronTrigger.getObject());
            scheduler.start();
        } catch (Exception e) {
            throw new SystemException(e);
        }
    }

    public void setTransactionRecovery(TransactionRecovery transactionRecovery) {
        this.transactionRecovery = transactionRecovery;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void setTransactionConfigurator(TransactionConfigurator transactionConfigurator) {
        this.transactionConfigurator = transactionConfigurator;
    }
}