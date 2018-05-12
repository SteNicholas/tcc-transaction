package org.mengyun.tcctransaction.support;

/**
 * Bean工厂
 */
public interface BeanFactory {

    /**
     * 获取指定类Bean实例
     *
     * @param clazz
     * @param <T>
     * @return
     */
    <T> T getBean(Class<T> clazz);

    /**
     * 判断是否为指定类工厂
     *
     * @param clazz
     * @param <T>
     * @return
     */
    <T> boolean isFactoryOf(Class<T> clazz);
}