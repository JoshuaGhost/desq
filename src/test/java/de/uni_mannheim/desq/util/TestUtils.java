package de.uni_mannheim.desq.util;

import de.uni_mannheim.desq.io.DelPatternReader;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.mining.Pattern;
import org.junit.rules.*;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Created by rgemulla on 18.07.2016.
 */
public class TestUtils {
    private static Logger logger = Logger.getLogger(TestUtils.class.getName());

    private static org.junit.rules.TemporaryFolder temporaryFolder;

    public static TemporaryFolder getTemporaryFolder() throws IOException {
        if (temporaryFolder == null) {
            temporaryFolder = new TemporaryFolder();
            temporaryFolder.create();
            logger.info("Creating temporary folder: " + temporaryFolder.getRoot());
        }
        return temporaryFolder;
    }

    public static File newTemporaryFile(String folder, String filename) throws IOException {
        getTemporaryFolder();
        File newFolder = new File(temporaryFolder.getRoot().getPath() + "/" + folder);
        if (!newFolder.exists()) newFolder.mkdir();
        File newFile = new File(newFolder.getPath() + "/" + filename);
        newFile.createNewFile();
        logger.info("Creating temporary file: " + newFile);
        return newFile;
    }

    public static String getPackageResourcesPath(Class clasz) {
        return "/" + clasz.getPackage().getName().replace(".", "/");
    }

    public static URL getPackageResourceUrl(Class clasz, String fileName) {
        String path = getPackageResourcesPath(clasz);
        return clasz.getResource(path + "/" + fileName);
    }

    public static File getPackageResource(Class clasz, String fileName) {
        return new File(getPackageResourceUrl(clasz, fileName).getPath());
    }

    public static void sortDelPatternFile(File file) throws IOException {
        // read the file into memory
        List<Pattern> patterns = new ArrayList<>();
        DelPatternReader reader = new DelPatternReader(new FileInputStream(file), true);
        reader.readAll(patterns);
        reader.close();

        // sort the patterns
        Collections.sort(patterns);

        // and write them back
        DelPatternWriter writer = new DelPatternWriter(new FileOutputStream(file, false), false);
        writer.writeAll(patterns);
        writer.close();
    }
}
