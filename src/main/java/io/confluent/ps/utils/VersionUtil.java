package io.confluent.ps.utils;

public class VersionUtil {

    public static String version(Class<?> cls) {
        Package pkg = cls.getPackage();
        String implementationVersion = pkg.getImplementationVersion();
        if (implementationVersion != null) {
            return implementationVersion;
        } else {
            // Read from properties file or return unknown
            ClassLoader classLoader = cls.getClassLoader();
            String versionProps = pkg.getName().replace('.', '/') + "/version.properties";
            try (java.io.InputStream input = classLoader.getResourceAsStream(versionProps)) {
                if (input != null) {
                    java.util.Properties prop = new java.util.Properties();
                    prop.load(input);
                    String version = prop.getProperty("version");
                    if (version != null) {
                        return version;
                    }
                }
            } catch (java.io.IOException e) {
                // Ignore and return unknown
            }
            return "unknown";
        }
    }

}
