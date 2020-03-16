// Databricks notebook source
case class MapEntry(key: String, value: Int)

val largeSeries = for (x <- 1 to 5000) yield MapEntry("k_%04d".format(x), x)
val largeDataFrame = sc.parallelize(largeSeries).toDF()
largeDataFrame.registerTempTable("largeTable")
display(sqlContext.sql("select * from largeTable"))

// COMMAND ----------

case class PivotEntry(key: String, series_grouping: String, value: Int)
val largePivotSeries = for (x <- 1 to 5000) yield PivotEntry("k_%03d".format(x % 200),"group_%01d".format(x % 3), x)
val largePivotDataFrame = sc.parallelize(largePivotSeries).toDF()
largePivotDataFrame.registerTempTable("table_to_be_pivoted")
display(sqlContext.sql("select * from table_to_be_pivoted"))

// COMMAND ----------

// MAGIC %sql select key, series_grouping, sum(value) from table_to_be_pivoted group by key, series_grouping order by key, series_grouping

// COMMAND ----------

case class SalesEntry(category: String, product: String, year: Int, salesAmount: Double)
val salesEntryDataFrame = sc.parallelize(
  SalesEntry("fruits_and_vegetables", "apples", 2012, 100.50) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2012, 100.75) :: 
  SalesEntry("fruits_and_vegetables", "apples", 2013, 200.25) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2013, 300.65) :: 
  SalesEntry("fruits_and_vegetables", "apples", 2014, 300.65) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2015, 100.35) ::
  SalesEntry("butcher_shop", "beef", 2012, 200.50) :: 
  SalesEntry("butcher_shop", "chicken", 2012, 200.75) :: 
  SalesEntry("butcher_shop", "pork", 2013, 400.25) :: 
  SalesEntry("butcher_shop", "beef", 2013, 600.65) :: 
  SalesEntry("butcher_shop", "beef", 2014, 600.65) :: 
  SalesEntry("butcher_shop", "chicken", 2015, 200.35) ::
  SalesEntry("misc", "gum", 2012, 400.50) :: 
  SalesEntry("misc", "cleaning_supplies", 2012, 400.75) :: 
  SalesEntry("misc", "greeting_cards", 2013, 800.25) :: 
  SalesEntry("misc", "kitchen_utensils", 2013, 1200.65) :: 
  SalesEntry("misc", "cleaning_supplies", 2014, 1200.65) :: 
  SalesEntry("misc", "cleaning_supplies", 2015, 400.35) ::
  Nil).toDF()
salesEntryDataFrame.registerTempTable("test_sales_table")
display(sqlContext.sql("select * from test_sales_table"))

// COMMAND ----------

// MAGIC %sql select cast(string(year) as date) as year, category, salesAmount from test_sales_table

// COMMAND ----------

// MAGIC %sql select * from test_sales_table

// COMMAND ----------

case class StateEntry(state: String, value: Int)
val stateRDD = sc.parallelize(
  StateEntry("MO", 1) :: StateEntry("MO", 10) ::
  StateEntry("NH", 4) ::
  StateEntry("MA", 8) ::
  StateEntry("NY", 4) ::
  StateEntry("CA", 7) ::  Nil).toDF()
stateRDD.registerTempTable("test_state_table")
display(sqlContext.sql("Select * from test_state_table"))

// COMMAND ----------

case class WorldEntry(country: String, value: Int)
val worldRDD = sc.parallelize(
  WorldEntry("USA", 1000) ::
  WorldEntry("JPN", 23) ::
  WorldEntry("GBR", 23) ::
  WorldEntry("FRA", 21) ::
  WorldEntry("TUR", 3) ::
  Nil).toDF()
display(worldRDD)

// COMMAND ----------

case class ScatterPlotEntry(key: String, a: Double, b: Double, c: Double)
val scatterPlotRDD = sc.parallelize(
  ScatterPlotEntry("k1", 0.2, 120, 1) :: ScatterPlotEntry("k1", 0.4, 140, 1) :: ScatterPlotEntry("k1", 0.6, 160, 1) :: ScatterPlotEntry("k1", 0.8, 180, 1) ::
  ScatterPlotEntry("k2", 0.2, 220, 1) :: ScatterPlotEntry("k2", 0.4, 240, 1) :: ScatterPlotEntry("k2", 0.6, 260, 1) :: ScatterPlotEntry("k2", 0.8, 280, 1) ::
  ScatterPlotEntry("k1", 1.2, 120, 1) :: ScatterPlotEntry("k1", 1.4, 140, 1) :: ScatterPlotEntry("k1", 1.6, 160, 1) :: ScatterPlotEntry("k1", 1.8, 180, 1) ::
  ScatterPlotEntry("k2", 1.2, 220, 2) :: ScatterPlotEntry("k2", 1.4, 240, 2) :: ScatterPlotEntry("k2", 1.6, 260, 2) :: ScatterPlotEntry("k2", 1.8, 280, 2) ::
  ScatterPlotEntry("k1", 2.2, 120, 1) :: ScatterPlotEntry("k1", 2.4, 140, 1) :: ScatterPlotEntry("k1", 2.6, 160, 1) :: ScatterPlotEntry("k1", 2.8, 180, 1) ::
  ScatterPlotEntry("k2", 2.2, 220, 3) :: ScatterPlotEntry("k2", 2.4, 240, 3) :: ScatterPlotEntry("k2", 2.6, 260, 3) :: ScatterPlotEntry("k2", 2.8, 280, 3) ::
  Nil).toDF()
display(scatterPlotRDD)

// COMMAND ----------

val rng = new scala.util.Random(0)
val points = sc.parallelize((0L until 1000L).map { x => (x/100.0, 4 * math.sin(x/100.0) + rng.nextGaussian()) }).toDF()
display(points)

// COMMAND ----------

case class HistogramEntry(key1: String, key2: String, value: Double)
val HistogramRDD = sc.parallelize(
  HistogramEntry("a", "x", 0.2) :: HistogramEntry("a", "x", 0.4) :: HistogramEntry("a", "x", 0.6) :: HistogramEntry("a", "x", 0.8) :: HistogramEntry("a", "x", 1.0) ::
  HistogramEntry("b", "z", 0.2) :: HistogramEntry("b", "x", 0.4) :: HistogramEntry("b", "x", 0.6) :: HistogramEntry("b", "y", 0.8) :: HistogramEntry("b", "x", 1.0) ::
  HistogramEntry("a", "x", 0.2) :: HistogramEntry("a", "y", 0.4) :: HistogramEntry("a", "x", 0.6) :: HistogramEntry("a", "x", 0.8) :: HistogramEntry("a", "x", 1.0) ::
  HistogramEntry("b", "x", 0.2) :: HistogramEntry("b", "x", 0.4) :: HistogramEntry("b", "x", 0.6) :: HistogramEntry("b", "z", 0.8) :: HistogramEntry("b", "x", 1.0) ::
  Nil).toDF()
display(HistogramRDD)

// COMMAND ----------

