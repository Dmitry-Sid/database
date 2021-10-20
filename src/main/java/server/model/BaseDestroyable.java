package server.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public abstract class BaseDestroyable extends BaseFilePathHolder implements Destroyable {
    private static final Logger log = LoggerFactory.getLogger(BaseDestroyable.class);

    protected final ObjectConverter objectConverter;
    protected final DestroyService destroyService;

    protected BaseDestroyable(String filePath, boolean init, ObjectConverter objectConverter, DestroyService destroyService, String... paths) {
        super(filePath);
        this.objectConverter = objectConverter;
        this.destroyService = destroyService;
        if (!init) {
            return;
        }
        if (destroyService != null) {
            destroyService.register(this);
        }
        if (paths == null) {
            return;
        }
        for (String path : paths) {
            createDirectoriesIfNotExist(path);
        }
    }

    @Override
    public void stop() {
        if (destroyService != null) {
            destroyService.unregister(this);
        }
    }

    private void createDirectoriesIfNotExist(String path) {
        final File file = new File(path);
        if (!new File(path).exists()) {
            log.info("creating directories for path " + path);
            Utils.createDirectoryTree(file);
            log.info("creation directories for path " + path + " is ended");
        }
    }
}
