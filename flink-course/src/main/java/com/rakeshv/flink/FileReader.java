package com.rakeshv.flink;

import java.net.URL;
import java.nio.file.Paths;

public class FileReader {

    public String getFilePath(String fileName) throws Exception {
        URL resource = getClass().getClassLoader().getResource(fileName);
        return Paths.get(resource.toURI()).toFile().getAbsolutePath();
    }
}
