package server.model;

public abstract class BaseFilePathHolder {
    protected final String filePath;

    protected BaseFilePathHolder(String filePath) {
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }
}
