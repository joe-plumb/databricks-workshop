# Databricks notebook source
# DBTITLE 1,Using SparkR in Databricks
# MAGIC %md
# MAGIC 
# MAGIC SparkR is an R package that provides a light-weight frontend to use Apache Spark from R. Starting with Spark 1.4.x, SparkR provides a distributed DataFrame implementation that supports operations like selection, filtering, aggregation etc. (similar to R data frames, dplyr) but on large datasets. SparkR also supports distributed machine learning using MLlib.
# MAGIC 
# MAGIC We will cover the following topics in this overview:
# MAGIC 
# MAGIC     SparkR in Databricks Notebooks
# MAGIC     Creating SparkR DataFrames
# MAGIC     From Local R Data Frames
# MAGIC     From Data Sources using Spark SQL
# MAGIC     Using Data Source Connectors with Spark Packages
# MAGIC     From Spark SQL Queries
# MAGIC     DataFrame Operations
# MAGIC     Selecting Rows & Columns
# MAGIC     Grouping & Aggregation
# MAGIC     Column Operations
# MAGIC     Machine Learning
# MAGIC 
# MAGIC 
# MAGIC SparkR offers distributed DataFrames that are syntax compatible with R data frames. You can also collect a SparkR DataFrame to local data frames.
# MAGIC Using SparkR you can access and manipulate very large data sets (e.g., terabytes of data) from distributed storage (e.g., Blob storage) or data warehouses (e.g., Hive)

# COMMAND ----------

# DBTITLE 1,SparkR in Databricks
# MAGIC %md SparkR started as a research project at AMPLab. With release of Spark 1.4.0, SparkR was inlined in Apache Spark. At that time Databricks released R Notebooks to be the first company that officially supports SparkR. To further facilitate usage of SparkR, Databricks R notebooks imported SparkR by default and provided a working sqlContext object.
# MAGIC 
# MAGIC SparkR and Databricks R notebooks evolved significantly since 2015. For the best experience, we highly recommend that you use the latest version of Spark on Databricks when you use either R or SparkR. Some of the most notable change in R and SparkR are:
# MAGIC 
# MAGIC     Starting with Spark 2.0, users do not need to explicitly pass a sqlContext object to every function call. This change reduced boilerplate code and made SparkR usercode more intuitive and readable. In this document we will follow the new syntax. For old syntax examples please refer to SparkR documentation prior to 2.0: SparkR Overview 
# MAGIC   
# MAGIC     Starting with Spark 2.2 Databricks notebooks do not import SparkR by default. Some of SparkR functions were conflicting with similarly named functions from other popular packages. Users who wish to use SparkR simply need to call library(SparkR) in their notebooks. The SparkR session is already configured and all SparkR functions will talk to your attached cluster using the exising session.

# COMMAND ----------

# DBTITLE 1,Load SparkR Library
library(SparkR)

# COMMAND ----------

# DBTITLE 1,Creating SparkR DataFrames
# MAGIC %md You can create DataFrames from a local R data frame, from data sources, or using Spark SQL queries.
# MAGIC 
# MAGIC The simplest way to create a DataFrame is to convert a local R data.frame into a SparkDataFrame. Specifically we can use createDataFrame and pass in the local R data.frame to create a SparkDataFrame. Like most other SparkR functions, createDataFrame syntax changed with Spark 2.0. You can see examples of this in the code snippet bellow.

# COMMAND ----------

df <- createDataFrame(faithful)

# Displays the content of the DataFrame to stdout
head(df)

# COMMAND ----------

# Select only the "eruptions" column
head(select(df, df$eruptions))

# COMMAND ----------

# You can also pass in column name as strings
head(select(df, "eruptions"))

# COMMAND ----------

# Filter the DataFrame to only retain rows with wait times shorter than 50 mins
head(filter(df, df$waiting < 50))

# COMMAND ----------

# DBTITLE 1,Grouping and Aggregation
# MAGIC %md SparkDataFrames support a number of commonly used functions to aggregate data after grouping. For example we can count the number of times each waiting time appears in the faithful dataset.

# COMMAND ----------

head(count(groupBy(df, df$waiting)))

# COMMAND ----------

# We can also sort the output from the aggregation to get the most common waiting times
waiting_counts <- count(groupBy(df, df$waiting))
head(arrange(waiting_counts, desc(waiting_counts$count)))

# COMMAND ----------

# DBTITLE 1,Column Operations
# MAGIC %md SparkR provides a number of functions that can be directly applied to columns for data processing and aggregation. The example below shows the use of basic arithmetic functions.

# COMMAND ----------

# Convert waiting time from hours to seconds.
# Note that we can assign this to a new column in the same DataFrame
df$waiting_secs <- df$waiting * 60
head(df)

# COMMAND ----------

# DBTITLE 1,Machine Learning
# MAGIC %md SparkR exposes most of MLLib algorithms. Under the hood, SparkR uses MLlib to train the model.
# MAGIC 
# MAGIC The example below shows the use of building a gaussian GLM model using SparkR. To run Linear Regression, set family to “gaussian”. To run Logistic Regression, set family to “binomial”. When using SparkML GLM SparkR automatically performs one-hot encoding of categorical features so that it does not need to be done manually. Beyond String and Double type features, it is also possible to fit over MLlib Vector features, for compatibility with other MLlib components.

# COMMAND ----------

# Create the DataFrame
df <- createDataFrame(iris)

# Fit a linear model over the dataset.
model <- glm(Sepal_Length ~ Sepal_Width + Species, data = df, family = "gaussian")

# Model coefficients are returned in a similar format to R's native glm().
summary(model)

# COMMAND ----------

# DBTITLE 1,Load DataFrames using SparkSQL
# MAGIC %md The general method for creating DataFrames from data sources is read.df. This method takes the path for the file to load and the type of data source. SparkR supports reading CSV, JSON, Text and Parquet files natively and through Spark Packages you can find data source connectors for popular file formats like CSV and Avro.

# COMMAND ----------

diamondsDF <- read.df("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv",source = "csv", header="true", inferSchema = "true")
head(diamondsDF)

# COMMAND ----------

# DBTITLE 1,Using GLM
# MAGIC %md glm fits a Generalized Linear Model, similar to R’s glm().

# COMMAND ----------

diamondsDF <- withColumnRenamed(diamondsDF, "", "rowID")

# Split data into Training set and Test set
trainingData <- sample(diamondsDF, FALSE, 0.7)
testData <- except(diamondsDF, trainingData)

# Exclude rowIDs
trainingData <- trainingData[, -1]
testData <- testData[, -1]

print(count(diamondsDF))
print(count(trainingData))
print(count(testData))

# COMMAND ----------

# DBTITLE 1,Training a Linear Regression model using glm()
# MAGIC %md
# MAGIC We will try to predict a diamond’s price from its features. We will do this by training a Linear Regression model using the training data.
# MAGIC 
# MAGIC Note that we have a mix of categorical features (for eg: cut - Ideal, Premium, Very Good...) and continuous features (for eg: depth, carat). Under the hood, SparkR automatically performs one-hot encoding of such features so that it does not have to be done manually.

# COMMAND ----------

# Indicate family = "gaussian" to train a linear regression model
lrModel <- glm(price ~ ., data = trainingData, family = "gaussian")

# Print a summary of trained linear regression model
summary(lrModel)

# COMMAND ----------

# Generate predictions using the trained Linear Regression model
predictions <- predict(lrModel, newData = testData)

# View predictions against mpg column
display(select(predictions, "price", "prediction"))

# COMMAND ----------

errors <- select(predictions, predictions$price, predictions$prediction, alias(predictions$price - predictions$prediction, "error"))
display(errors)

# COMMAND ----------

# MAGIC %md You can register DataFrames for use with SparkSQL with registerTempTable

# COMMAND ----------

# Calculate RMSE
head(select(errors, alias(sqrt(sum(errors$error^2 , na.rm = TRUE) / nrow(errors)), "RMSE")))

# COMMAND ----------

# DBTITLE 1,Sparklyr
# MAGIC %md R is mostly optimized to help you write data analysis code quickly and readably. Apache Spark is designed to analyze huge datasets quickly. The sparklyr package lets you write dplyr R code that runs on a Spark cluster, giving you the best of both worlds.

# COMMAND ----------

# DBTITLE 1,Installing sparklyr
install.packages("sparklyr")

# COMMAND ----------

library(sparklyr)
sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Using sparklyr APIs
library(dplyr)
iris_tbl <- copy_to(sc, iris)
iris_summary <- iris_tbl %>% 
    mutate(Sepal_Width = ROUND(Sepal_Width * 2) / 2) %>% 
    group_by(Species, Sepal_Width) %>% 
    summarize(count = n(),
Sepal_Length = mean(Sepal_Length),
stdev = sd(Sepal_Length)) %>% collect

library(ggplot2)
ggplot(iris_summary, 
   aes(Sepal_Width, Sepal_Length, color = Species)) + 
    geom_line(size = 1.2) +
    geom_errorbar(aes(
 ymin = Sepal_Length - stdev,
ymax = Sepal_Length + stdev),
   width = 0.05) +
    geom_text(aes(label = count), 
vjust = -0.2, hjust = 1.2, color = "black") +
    theme(legend.position="top")
