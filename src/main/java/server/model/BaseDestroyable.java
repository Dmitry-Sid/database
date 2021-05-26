package server.model;

public abstract class BaseDestroyable implements Destroyable {
    private final DestroyService destroyService;

    protected BaseDestroyable(DestroyService destroyService) {
        this.destroyService = destroyService;
        if (destroyService != null) {
            destroyService.register(this);
        }
    }

    @Override
    public void stop() {
        if (destroyService != null) {
            destroyService.unregister(this);
        }
    }
}
