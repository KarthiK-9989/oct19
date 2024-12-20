//import org.apache.spark.sql.SparkSession
//
//object nine {
//  def main(args: Array[String]): Unit = {
//    // Step 1: Create a SparkSession
//    val spark = SparkSession.builder()
//      .appName("Dataset Example")
//      .master("local[*]")
//      .getOrCreate()
//
//    // Import implicit encoders for converting to Dataset
//    import spark.implicits._
//
//    // Step 2: Define a case class to represent the schema
//    case class Person(id: Int, name: String, age: Int)
//
//    // Step 3: Create a sequence of data
//    val data = Seq(
//      Person(1, "Alice", 29),
//      Person(2, "Bob", 35),
//      Person(3, "Charlie", 23)
//    )
//
//    // Step 4: Create a Dataset from the data
//    val dataset = spark.createDataset(data)
//
//    // Step 5: Perform operations on the Dataset
//    dataset.show()
//
//    // Step 6: Stop the SparkSession
//    spark.stop()
//  }
//}
