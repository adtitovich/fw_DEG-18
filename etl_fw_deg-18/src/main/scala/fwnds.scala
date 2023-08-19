/* sbt package


 YOUR_SPARK_HOME/bin/spark-submit \
  --class "fwnds" \
  etl_fw_deg-18_2.13-1.0.jar
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.util.Properties


object fwnds {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("FinalWork ETL NDS")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    println("- - - - - - - - - - - - - - - - - - - ")
    println("Spark Session has been created")

    val csvFile = "/opt/workspace/supermarket_sales.csv" // имя файл для загрузки
    val csvNullFile = "/opt/workspace/ndsnullvalues.csv" // имя файла для записи строк со значениями NULL

    // схема 
    val schema = new StructType()
        .add("Invoice ID", StringType)
        .add("Branch", StringType)
        .add("City", StringType)
        .add("Customer type", StringType)
        .add("Gender", StringType)
        .add("Product line", StringType)
        .add("Unit price", DecimalType(20, 2))
        .add("Quantity", IntegerType)
        .add("Tax 5%", DecimalType(20, 4))
        .add("Total", StringType)
        .add("Date", StringType)
        .add("Time", StringType)
        .add("Payment", StringType)
        .add("cogs", StringType)
        .add("gross margin percentage", StringType)
        .add("gross income", StringType)
        .add("Rating", DecimalType(3, 1))
    
    // новые названия столбцов	
    val newColumns = Seq("invoice", "branch", "city", "customertype", "gender", "productline", "unitprice", "quantity",
           "tax", "total", "date", "time", "paymenttype", "cogs", "grossmargin", "grossincome", "rating")
    
    println("- - - - - - - - - - - - - - - - - - - ")
    println("Read " + csvFile)
    // считытываем файл в DataFrame, переименовываем столбцы и удаляем столбцы, которые не будем использовать
    val df = spark.read
        .option("header", value = true)
        .option("inferschema", value = true)
        .schema(schema)
        .csv(csvFile).toDF(newColumns:_*).drop($"total", $"cogs", $"grossmargin", $"grossincome")
        
    // найдем строки со значениями NULL (могут получиться при несовпадении типов)
    val nullValues = df.filter(row => row.anyNull)
    val nullValuesCount = nullValues.count()
    
    println("- - - - - - - - - - - - - - - - - - - ")
    println("Count of rows with NULL values - " + nullValuesCount.toString)
    if (nullValuesCount > 0) {
       // запишем в отдельный CSV для анализа
       nullValues.write.option("header", value = true).csv(csvNullFile)
       println("CVS file with NULL values - " + csvNullFile)
    }
    

    println("- - - - - - - - - - - - - - - - - - - ")
    println("Transform data")

    // исключаем строки с Null значениями
    val dfnn = df.join(nullValues, df.columns.toSeq, "leftanti")
    
    // создаем DataFrame с название городов, добавляя к нему первичный ключ
    val dfCity = dfnn.select($"city").distinct().withColumn("city_id",monotonically_increasing_id())
    
    // создаем DataFrame с гендерами, добавляя к нему первичный ключ
    val dfGender = dfnn.select($"gender").distinct().withColumn("gender_id",monotonically_increasing_id())
    
    // создаем DataFrame с типами пользователей, добавляя к нему первичный ключ
    val dfCustomertype = dfnn.select($"customertype").distinct().withColumn("customertype_id",monotonically_increasing_id())
    
    // создаем DataFrame типов товаров, добавляя к нему первичный ключ
    val dfProductline = dfnn.select($"productline").distinct().withColumn("productline_id", monotonically_increasing_id())
    
    // создаем DataFrame с типами платежей, добавляя к нему первичный ключ
    val dfPaymenttype = dfnn.select($"paymenttype").distinct().withColumn("paymenttype_id", monotonically_increasing_id())
    
    //  создаем DataFrame филиалов, добавляя к нему первичный ключ
    val dfBranch = dfnn.select($"branch", $"city").distinct().withColumn("branch_id",monotonically_increasing_id())
        .join(dfCity, Seq("city"), "left")
    
    // создаем DataFrame товаров, добавляя к нему первичный ключ
    val dfProduct = dfnn.select($"unitprice", $"productline").distinct().withColumn("product_id",monotonically_increasing_id())
        .join(dfProductline, Seq("productline"), "left")
    
    // создаем DataFrame с покупателями, добавляя к нему первичный ключ
    val dfCustomer = dfnn.select($"customertype", $"gender", $"rating").distinct().withColumn("customer_id", monotonically_increasing_id())
        .join(dfGender, Seq("gender"), "left")
        .join(dfCustomertype, Seq("customertype"), "left")
        
    // создаем DataFrame c заказами, добавляя к нему первичный ключ
    val dfOrder = dfnn.withColumn("dt", to_timestamp(concat($"Date", lit(" "), $"Time"), "M/d/yyyy HH:mm"))
        .withColumn("order_id", monotonically_increasing_id())
        .join(dfBranch, Seq("branch", "city"), "left")
        .join(dfCustomer, Seq("customertype", "gender", "rating"), "left")
        .join(dfProduct, Seq("productline", "unitprice"), "left")
        .join(dfPaymenttype, Seq("paymenttype"), "left")


    // инициализация записи в postgresql
    val dbProps = new Properties()
    dbProps.put("connectionURL", "jdbc:postgresql://192.168.0.25:5433/supermarket")
    dbProps.put("driver", "org.postgresql.Driver")
    dbProps.put("user", "postgres")
    dbProps.put("password", "example")

    val connectionURL = dbProps.getProperty("connectionURL")

    println("- - - - - - - - - - - - - - - - - - - ")
    println("Write to " + connectionURL)
    
    // пишем в БД
    dfCity.write.mode("append")
        .jdbc(connectionURL,"nds.city",dbProps)
    dfGender.write.mode("append")
        .jdbc(connectionURL,"nds.gender",dbProps)
    dfCustomertype.write.mode("append")
        .jdbc(connectionURL,"nds.customertype",dbProps)
    dfProductline.write.mode("append")
        .jdbc(connectionURL,"nds.productline",dbProps)
    dfPaymenttype.write.mode("append")
        .jdbc(connectionURL,"nds.paymenttype",dbProps)
    dfBranch.select($"branch_id", $"city_id", $"branch").write.mode("append")
        .jdbc(connectionURL,"nds.branch",dbProps)
    dfProduct.select($"product_id", $"productline_id", $"unitprice").write.mode("append")
        .jdbc(connectionURL,"nds.product",dbProps)
    dfCustomer.select($"customer_id", $"gender_id", $"customertype_id", $"rating").write.mode("append")
        .jdbc(connectionURL,"nds.customer",dbProps)
    dfOrder.select($"order_id", $"branch_id", $"customer_id", $"product_id", $"paymenttype_id", $"invoice", 
        $"quantity", $"tax", $"dt").write.mode("append")
        .jdbc(connectionURL,"nds.order",dbProps)
    
    spark.stop()
  }
}

