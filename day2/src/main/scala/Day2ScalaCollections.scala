object Day2ScalaCollections {

  def main(args: Array[String]): Unit = {

    println("===== DAY 2: SCALA COLLECTIONS PRACTICE =====\n")

    // --------------------------------------------------
    // 1. LIST - map, filter, flatMap, reduce
    // --------------------------------------------------

    val sales = List(100, 250, 150, 300, 200)

    println("Original Sales:")
    println(sales)

    // map - increase every sale by 10%
    val increasedSales = sales.map(amount => amount * 1.10)

    println("\nSales after 10% increase:")
    println(increasedSales)

    // filter - select sales greater than 200
    val highSales = sales.filter(amount => amount > 200)

    println("\nSales greater than 200:")
    println(highSales)

    // flatMap - split each sale into smaller values
    val saleBreakdown = sales.flatMap(amount => List(amount, amount / 2))

    println("\nSale Breakdown using flatMap:")
    println(saleBreakdown)

    // reduce - calculate total sales
    val totalSales = sales.reduce((a, b) => a + b)

    println("\nTotal Sales using reduce:")
    println(totalSales)


    // --------------------------------------------------
    // 2. VECTOR - Indexed Customer Records
    // --------------------------------------------------

    val customers = Vector(
      (101, "Aman"),
      (102, "Rahul"),
      (103, "Priya"),
      (104, "Neha")
    )

    println("\n===== VECTOR: CUSTOMER RECORDS =====")

    println("All Customers:")
    customers.foreach(println)

    println("\nCustomer at index 2:")
    println(customers(2))

    /*
     * Vector is useful when we need indexed access.
     * It provides efficient access to elements by index
     * while maintaining immutable collection behavior.
     */


    // --------------------------------------------------
    // 3. MAP - Product Quantities and Prices
    // --------------------------------------------------

    val productQuantities = Map(
      "Laptop" -> 2,
      "Phone" -> 5,
      "Headphones" -> 10,
      "Keyboard" -> 4
    )

    val productPrices = Map(
      "Laptop" -> 60000,
      "Phone" -> 25000,
      "Headphones" -> 2000,
      "Keyboard" -> 1500
    )

    println("\n===== MAP: PRODUCT SALES =====")

    println("Product Quantities:")
    productQuantities.foreach(println)

    println("\nProduct Prices:")
    productPrices.foreach(println)

    val productValues = productQuantities.map {
      case (product, quantity) =>
        val price = productPrices.getOrElse(product, 0)
        product -> (quantity * price)
    }

    println("\nTotal Value by Product:")
    productValues.foreach(println)

    val mapTotalSales = productValues.values.reduce(_ + _)

    println("\nTotal Product Sales:")
    println(mapTotalSales)


    // --------------------------------------------------
    // 4. FOR-COMPREHENSION - Customers and Orders
    // --------------------------------------------------

    val orders = List(
      (101, "Laptop", 1),
      (102, "Phone", 2),
      (101, "Keyboard", 2),
      (103, "Headphones", 3),
      (104, "Phone", 1)
    )

    println("\n===== FOR-COMPREHENSION =====")

    val customerOrders =
      for {
        customer <- customers
        order <- orders
        if customer._1 == order._1
      } yield (customer._2, order._2, order._3)

    println("Customer Orders:")
    customerOrders.foreach(println)


    // --------------------------------------------------
    // 5. DAILY SALES SUMMARY
    // --------------------------------------------------

    val dailySales = List(
      ("2026-09-08", "Laptop", 1, 60000),
      ("2026-09-08", "Phone", 2, 25000),
      ("2026-09-08", "Headphones", 3, 2000),
      ("2026-09-08", "Keyboard", 2, 1500)
    )

    println("\n===== DAILY SALES SUMMARY =====")

    val dailyTotals =
      dailySales
        .map {
          case (date, product, quantity, price) =>
            (date, quantity * price)
        }
        .groupBy(_._1)
        .map {
          case (date, sales) =>
            date -> sales.map(_._2).sum
        }

    dailyTotals.foreach {
      case (date, total) =>
        println(s"$date -> ₹$total")
    }

    val grandTotal = dailyTotals.values.sum

    println(s"\nGrand Total Sales: ₹$grandTotal")

    println("\n===== PROGRAM COMPLETED =====")
  }
}
