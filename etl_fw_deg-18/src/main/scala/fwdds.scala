/* sbt package


 YOUR_SPARK_HOME/bin/spark-submit \
  --class "fwdds" \
  etl_fw_deg-18_2.13-1.0.jar
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Properties


object fwdds {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("FinalWork ETL NDS")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    println("- - - - - - - - - - - - - - - - - - - ")
    println("Spark Session has been created")

    // инициализация записи в postgresql
    val dbProps = new Properties()
    dbProps.put("connectionURL", "jdbc:postgresql://192.168.0.25:5433/supermarket")
    dbProps.put("driver", "org.postgresql.Driver")
    dbProps.put("user", "postgres")
    dbProps.put("password", "example")

    val connectionURL = dbProps.getProperty("connectionURL")

    println("- - - - - - - - - - - - - - - - - - - ")
    println("Read from " + connectionURL)

    // считытываем таблицы
    val dfCity = spark.read.jdbc(connectionURL, "nds.city", dbProps)
    val dfGender = spark.read.jdbc(connectionURL, "nds.gender", dbProps)
    val dfCustomertype = spark.read.jdbc(connectionURL, "nds.customertype", dbProps)
    val dfProductline = spark.read.jdbc(connectionURL, "nds.productline", dbProps)
    val dfPaymenttype = spark.read.jdbc(connectionURL, "nds.paymenttype", dbProps)
    val dfBranch = spark.read.jdbc(connectionURL, "nds.branch", dbProps)
    val dfProduct = spark.read.jdbc(connectionURL, "nds.product", dbProps)
    val dfCustomer = spark.read.jdbc(connectionURL, "nds.customer", dbProps)
    val dfOrder = spark.read.jdbc(connectionURL, "nds.order", dbProps)

    println("- - - - - - - - - - - - - - - - - - - ")
    println("Transform data")

    // dfDim_Branch -- справочник магазинов
    val dfDim_Branch = dfBranch.withColumn("id", monotonically_increasing_id())
      .join(dfCity, Seq("city_id"), "left")
      .drop($"city_id")

    // dfDim_Customer -- справочник покупателей
    val dfDim_Customer_raw = dfCustomer.withColumn("id", monotonically_increasing_id())
      .join(dfGender, Seq("gender_id"), "left")
      .join(dfCustomertype, Seq("customertype_id"), "left")
      .drop($"gender_id", $"customertype_id")

    // проверяем на качество данных dfDim_Customer_raw
    val dfCustomer_reject = dfDim_Customer_raw.where(!$"rating".between(1, 10))
    val dfDim_Customer = dfDim_Customer_raw.join(dfCustomer_reject, dfDim_Customer_raw.columns.toSeq,"leftanti")

    // dfDim_Product -- справочник товаров
    val dfDim_Product_raw = dfProduct.withColumn("id", monotonically_increasing_id())
      .join(dfProductline, Seq("productline_id"), "left")
      .drop($"productline_id")

    // проверяем на качество данных dfDim_Product_raw
    val dfProduct_reject = dfDim_Product_raw.where($"unitprice" <= 0.0)
    val dfDim_Product = dfDim_Product_raw.join(dfProduct_reject, dfDim_Product_raw.columns.toSeq,"leftanti")


    // Dim_Paymenttype -- справочник типов платежей
    val dfDim_Paymenttype = dfPaymenttype.withColumn("id", monotonically_increasing_id())


    // Fact_Order - покупки
    val dfFact_Order_raw = dfOrder.withColumn("date_key", date_format($"dt", "yyyyMMdd").cast("Integer"))
      .join(dfDim_Branch.select($"id".as("branch_key"), $"branch_id"), Seq("branch_id"), "left").drop($"branch_id")
      .join(dfDim_Customer.select($"id".as("customer_key"), $"customer_id"), Seq("customer_id"), "left").drop($"customer_id")
      .join(dfDim_Paymenttype.select($"id".as("paymenttype_key"), $"paymenttype_id"), Seq("paymenttype_id"), "left").drop($"paymenttype_id")
      .join(dfDim_Product.select($"id".as("product_key"), $"product_id", $"unitprice"), Seq("product_id"), "left").drop($"product_id")
      .withColumn("salesprice", $"unitprice" * $"quantity")
      .withColumn("total", $"salesprice" + $"tax")
      .drop($"order_id")

    // проверяем на качество данных dfFact_Order_raw
    val today = java.time.LocalDate.now
    val dfFact_reject = dfFact_Order_raw.where(
      $"dt" > today
      || $"quantity" <= 0
      || !$"tax".between(0, $"salesprice")
      || !$"invoice".rlike("^\\d{3}\\-\\d{2}\\-\\d{4}$"))
    val dfFact_Order = dfFact_Order_raw.join(dfFact_reject, dfFact_Order_raw.columns.toSeq, "leftanti")

    println("- - - - - - - - - - - - - - - - - - - ")
    println("Write to " + connectionURL)

    // пишем в БД
    dfDim_Branch.write.mode("append")
      .jdbc(connectionURL, "dds.Dim_Branch", dbProps)

    dfDim_Customer.write.mode("append")
      .jdbc(connectionURL, "dds.Dim_Customer", dbProps)

    dfCustomer_reject.drop($"id").write.mode("append")
      .jdbc(connectionURL, "rejected.customer", dbProps)

    dfDim_Product.write.mode("append")
      .jdbc(connectionURL, "dds.Dim_Product", dbProps)

    dfProduct_reject.drop($"id").write.mode("append")
      .jdbc(connectionURL, "rejected.product", dbProps)

    dfDim_Paymenttype.write.mode("append")
      .jdbc(connectionURL, "dds.Dim_Paymenttype", dbProps)

    dfFact_Order.write.mode("append")
      .jdbc(connectionURL, "dds.Fact_Order", dbProps)

    dfFact_reject
      .drop($"date_key", $"branch_key", $"customer_key", $"product_key", $"paymenttype_key")
      .write.mode("append")
      .jdbc(connectionURL, "rejected.order", dbProps)


    spark.stop()

  }
}
