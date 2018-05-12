package org.mengyun.tcctransaction.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 工厂构造器
 */
public final class FactoryBuilder {

    private FactoryBuilder() {

    }

    /**
     * Bean工厂集合
     */
    private static List<BeanFactory> beanFactories = new ArrayList<BeanFactory>();

    /**
     * 类与单例工厂映射
     */
    private static ConcurrentHashMap<Class, SingeltonFactory> classFactoryMap = new ConcurrentHashMap<Class, SingeltonFactory>();

    /**
     * 获取指定类单例工厂
     *
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> SingeltonFactory<T> factoryOf(Class<T> clazz) {
        if (!classFactoryMap.containsKey(clazz)) {
            //遍历Bean工厂集合,判断是否为指定类工厂,是则按照指定类与Bean实例创建单例工厂新增映射关联
            for (BeanFactory beanFactory : beanFactories) {
                if (beanFactory.isFactoryOf(clazz)) {
                    classFactoryMap.putIfAbsent(clazz, new SingeltonFactory<T>(clazz, beanFactory.getBean(clazz)));
                }
            }
            //类与单例工厂映射不包含指定类的键,按照指定类创建单例工厂新增指定类与新建单例工厂映射
            if (!classFactoryMap.containsKey(clazz)) {
                classFactoryMap.putIfAbsent(clazz, new SingeltonFactory<T>(clazz));
            }
        }

        return classFactoryMap.get(clazz);
    }

    /**
     * 注册指定Bean工厂到工厂构造器Bean工厂集合
     *
     * @param beanFactory
     */
    public static void registerBeanFactory(BeanFactory beanFactory) {
        beanFactories.add(beanFactory);
    }

    /**
     * 单例工厂
     *
     * @param <T>
     */
    public static class SingeltonFactory<T> {
        /**
         * 单例
         */
        private volatile T instance = null;

        /**
         * 类名
         */
        private String className;

        public SingeltonFactory(Class<T> clazz, T instance) {
            this.className = clazz.getName();
            this.instance = instance;
        }

        public SingeltonFactory(Class<T> clazz) {
            this.className = clazz.getName();
        }

        /**
         * 获取单例
         *
         * @return
         */
        public T getInstance() {
            if (instance == null) {
                synchronized (SingeltonFactory.class) {
                    if (instance == null) {
                        try {
                            ClassLoader loader = Thread.currentThread().getContextClassLoader();

                            Class<?> clazz = loader.loadClass(className);

                            instance = (T) clazz.newInstance();
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to create an instance of " + className, e);
                        }
                    }
                }
            }

            return instance;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;

            SingeltonFactory that = (SingeltonFactory) other;

            if (!className.equals(that.className)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return className.hashCode();
        }
    }
}