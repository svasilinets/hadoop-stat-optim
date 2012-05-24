import org.apache.hadoop.util.ProgramDriver;

/**
 * User: svasilinets
 * Date: 27.03.12
 * Time: 10:54
 */
public class ToolsDriver {

    public static void main(String[] args) {
        ProgramDriver programDriver = new ProgramDriver();
        try {
            programDriver.addClass("compare", Compare.class, "shows statistic");
            programDriver.addClass("ckeys", CompareKeys.class, "compare key between jobs");
            programDriver.addClass("r", KeysOnReducers.class, "shows number of pairs(key,value) on reducers");
            programDriver.driver(args);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

    }
}
