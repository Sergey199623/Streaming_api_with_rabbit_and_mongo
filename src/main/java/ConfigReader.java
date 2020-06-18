import javax.validation.constraints.Null;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class ConfigReader {
    private final String path = "src/main/resources/config.properties";
    private final Properties props = new Properties();

    private static ConfigReader instance = null;

    private ConfigReader() {
        try (InputStream input = new FileInputStream(path)) {
            props.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static ConfigReader getInstance(){
        if(instance == null){
            synchronized (ConfigReader.class) {
                instance = new ConfigReader();
            }
        }
        return instance;
    }

    public String getProp(String propName){
        return props.getProperty(propName);
    }

}
