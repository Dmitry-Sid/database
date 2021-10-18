package server.model.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import server.model.BaseFilePathHolder;
import server.model.TableServiceFactory;
import server.model.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class TableServiceFactoryImpl extends BaseFilePathHolder implements TableServiceFactory, BeanFactoryAware {
    private static final Logger log = LoggerFactory.getLogger(TableServiceFactoryImpl.class);
    private static final Object LOCK = new Object();

    private BeanFactory beanFactory;

    public TableServiceFactoryImpl(String filePath) {
        super(filePath);
    }

    @Override
    public void createServices(String tableName, Consumer<Map<Class<?>, Object>> consumer, Class<?>... classes) {
        synchronized (LOCK) {
            final DefaultListableBeanFactory defaultListableBeanFactory = (DefaultListableBeanFactory) beanFactory;
            final BeanDefinition beanDefinition = defaultListableBeanFactory.getBeanDefinition("tableService");
            final String filePath = Utils.getFullPath(TableServiceFactoryImpl.this.filePath, tableName);
            beanDefinition.getConstructorArgumentValues().addIndexedArgumentValue(0, filePath);
            beanDefinition.getConstructorArgumentValues().addIndexedArgumentValue(1, true);
            log.info("bean tableService initialized with filePath " + filePath);
            defaultListableBeanFactory.registerBeanDefinition("tableService", beanDefinition);
            final Map<Class<?>, Object> map = new HashMap<>();
            for (Class<?> clazz : classes) {
                map.put(clazz, beanFactory.getBean(clazz));
            }
            consumer.accept(map);
        }
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }
}