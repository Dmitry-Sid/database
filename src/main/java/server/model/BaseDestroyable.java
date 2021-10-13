package server.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public abstract class BaseDestroyable implements Destroyable {
    private static final Logger log = LoggerFactory.getLogger(BaseDestroyable.class);

    private final DestroyService destroyService;

    protected BaseDestroyable(DestroyService destroyService, String... paths) {
        this.destroyService = destroyService;
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
