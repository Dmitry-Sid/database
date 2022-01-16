package server.model.lock;

abstract class BaseReadWriteLock<T> implements ReadWriteLock<T> {
    private static final boolean DEBUG_MODE = false;

    private String printStackTrace() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Thread ").append(Thread.currentThread().getName());
        for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
            sb.append("\t").append(ste).append("\n");
        }
        return sb.toString();
    }

    protected enum Type {
        Read, Write
    }

    protected abstract class BaseInnerLock implements Lock<T> {
        private final Type type;

        protected BaseInnerLock(Type type) {
            this.type = type;
        }

        @Override
        public void lock(T value) {
            if (DEBUG_MODE) {
                System.out.println("try to acquire lock, value " + value + ", type " + type + ", " + printAdditionalInfo(value) + printStackTrace());
            }
            innerLock(value);
            if (DEBUG_MODE) {
                System.out.println("lock acquired, value " + value + ", type " + type + ", " + printAdditionalInfo(value) + printStackTrace());
            }
        }

        protected abstract void innerLock(T value);

        @Override
        public void unlock(T value) {
            if (DEBUG_MODE) {
                System.out.println("try to release lock, value " + value + ", type " + type + ", " + printAdditionalInfo(value) + printStackTrace());
            }
            innerUnLock(value);
            if (DEBUG_MODE) {
                System.out.println("lock released, value " + value + ", type " + type + ", " + printAdditionalInfo(value) + printStackTrace());
            }
        }

        protected String printAdditionalInfo(T value) {
            return "";
        }

        protected abstract void innerUnLock(T value);
    }
}