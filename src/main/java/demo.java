import com.amazon.deequ.examples.ExampleUtils;
import org.apache.spark.sql.SparkSession;
import com.amazon.deequ.examples.Item;
import scala.collection.JavaConverters;
import java.util.Collections;
import java.util.List;

public class demo {
    public static void main(String[] args) {
        System.out.println("Hello");
        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("test")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        Item item = new Item(1, "Thingy A", "awesome thing.", "high", 0);
        List<Item> itemList = Collections.singletonList(item);
        ExampleUtils.itemsAsDataframe(session, JavaConverters.asScalaBuffer(itemList).toSeq());
    }
}
