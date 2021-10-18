package server.model.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import server.model.BaseFilePathHolder;
import server.model.TableServiceFactory;
import server.model.Utils;

public class TableServiceFactoryImpl extends BaseFilePathHolder implements TableServiceFactory {
    private static final Logger log = LoggerFactory.getLogger(TableServiceFactoryImpl.class);

    @Autowired
    private BeanFactory beanFactory;

    public TableServiceFactoryImpl(String filePath) {
        super(filePath);
    }

    @Override
    public <T> T getService(String tableName, Class<T> clazz) {
        return beanFactory.getBean(clazz);
    }

    @Override
    public void createServices(String tableName) {
        final DefaultListableBeanFactory defaultListableBeanFactory = (DefaultListableBeanFactory) beanFactory;
        final BeanDefinition beanDefinition = defaultListableBeanFactory.getBeanDefinition("baseDestroyable");
        final String filePath = Utils.getFullPath(TableServiceFactoryImpl.this.filePath, tableName);
        beanDefinition.getConstructorArgumentValues().addIndexedArgumentValue(0, filePath);
        beanDefinition.getConstructorArgumentValues().addIndexedArgumentValue(1, true);
        log.info("bean baseDestroyable initialized with filePath " + filePath);
        defaultListableBeanFactory.registerBeanDefinition("baseDestroyable", beanDefinition);
    }
}