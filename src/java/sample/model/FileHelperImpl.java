package sample.model;

import sample.model.pojo.RowAddress;

import java.io.*;

public class FileHelperImpl implements FileHelper {
    @Override
    public void collectFile(RowAddress rowAddress, InputOutputConsumer inputOutputConsumer) {
        LockKeeper.getFileLock().lock(rowAddress.getFilePath());
        try {
            final File fileInput = new File(rowAddress.getFilePath());
            final File fileOutput = new File(rowAddress.getFilePath() + ".tmp");
            try (FileInputStream input = new FileInputStream(fileInput);
                 BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(fileOutput), 10000)) {
                long position = 0;
                int bit;
                while ((bit = input.read()) != -1) {
                    if (position == rowAddress.getPosition()) {
                        inputOutputConsumer.accept(input, output);
                    } else {
                        output.write(bit);
                    }
                    position++;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            fileInput.delete();
            fileOutput.renameTo(fileInput);
        } finally {
            LockKeeper.getFileLock().unlock(rowAddress.getFilePath());
        }
    }
}
