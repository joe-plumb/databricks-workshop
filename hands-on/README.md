# Azure Databricks Labs

These labs are written as a follow along guide for you to get some hands on experience of working in and with Azure Databricks. The recommendation is to work with this hands on guide to write the code yourself into your own notebook(s). You are encouraged to experiment as you go by changing and modifying the code - it's the best way to learn!

## Pre-requisistes 
Attendees will need:
- Microsoft ID 
- Azure Subscription

Follow the below links to get your environment set up:
- [Workspace setup](https://docs.microsoft.com/en-us/azure/databricks/scenarios/quickstart-create-databricks-workspace-portal?tabs=azure-portal)
- [Cluster setup](https://docs.microsoft.com/en-us/azure/databricks/clusters/create) 
    - NB while setting up your cluster, disable autoscaling to reduce costs.
- Import the .dbc

## Lab 1: Introduction to Azure Databricks and Spark Notebooks

### What is Apache Spark notebook?
A notebook is a collection of cells. These cells are run to execute code, to render formatted text, or to display graphical visualizations.
### Create a new Notebook
1. In the Azure portal, click All resources menu on the left side navigation and select the Databricks workspace you created in the last unit.
2. Select Launch Workspace to open your Databricks workspace in a new tab.
3. On the left-hand menu of your Databricks workspace, select Home.
4. Right-click on your home folder.
5. Select Create.
6. Select Notebook.
7. The menu option to create a new notebook
8. Name your notebook First Notebook.
9. Set the Language to Python.
10. Select the cluster to which to attach this notebook.
11. Select Create.

Now that you've created your notebook, let's use it to run some code.


## Technical Accomplishments from this lab:
- Set the stage for learning on the Databricks platform
- Demonstrate how to develop & execute code within a notebook
- Introduce the Databricks File System (DBFS)
- Introduce `dbutils`
- Review the various "Magic Commands"
- Review various built-in commands that facilitate working with the notebooks

## Step 1 : How to run commands 
-Each notebook is tied to a specific language: **Scala**, **Python**, **SQL** or **R**
- Run the cell below using one of the following options:
  - **CTRL+ENTER** or **CMD+RETURN**
  - **SHIFT+ENTER** or **SHIFT+RETURN** to run the cell and move to the next one
  - Using **Run Cell**, **Run All Above** or **Run All Below** as seen here
  
  Try running the code below in your notebook
  
```
  print("Im running a new Notebook!")
```

## Step 2 : Magic Commands
- Magic Commands are specific to the Databricks notebooks
- They are very similar to Magic Commands found in comparable notebook products
- These are built-in commands that do not apply to the notebook's default language
- A single percent (%) symbol at the start of a cell identifies a Magic Commands

Magic commands allow to run shell commands or execution of code in languages other than the notebook's default:

- `%sh` : To execute shell commands
- `%python` 
- `%scala`
- `%r`
- `%sql`


We can use `%scala` to run the above print command in scala:

```
%scala 
println("I's running a new Notebook!")
```
  
  ###  Other key Magic Commands
  1) `%md` : allows us to render Markdown in a cell
  2) `%run` : You can run a notebook from another notebook by using this Magic Command. All variables & functions defined in that other notebook will become available in your current notebook
  
  
 ## Step 3 : Databricks File System and Utilities.
 ### Databricks File System
- DBFS is a layer over a cloud-based object store
- Files in DBFS are persisted to the object store
- The lifetime of files in the DBFS are **NOT** tied to the lifetime of our cluster
#### Mounting Data into DBFS
- Mounting other object stores into DBFS gives Databricks users access via the file system
- This is just one of many techniques for pulling data into Spark
- The datasets needed for this class have already been mounted for us with the call to %run "../Includes/Classroom Setup"
- We will confirm that in just a few minutes

### Databricks Utilties 
- You can access the DBFS through the Databricks Utilities class (and other file IO routines).
- An instance of DBUtils is already declared for us as `dbutils`.
- For in-notebook documentation on DBUtils you can execute the command `dbutils.help()`

#### Additional help is available for each sub-utility:
- `dbutils.fs.help()` : Manipulates the Databricks filesystem (DBFS) from the console
- `dbutils.meta.help()` : Methods to hook into the compiler (EXPERIMENTAL
- `dbutils.notebook.help()`: Utilities for the control flow of a notebook (EXPERIMENTAL)
- `dbutils.widgets.help()`:  Methods to create and get bound value of input widgets inside notebooks

Let's take a look at the file system utilities, `dbutils.fs`

Run the following command 
```
dbutils.fs.help()

```
#### Mounts in dbutills
For the purpose of this demo all the data has already been mounted to the `training` datastore. 
We can use dbutils.fs.mounts() to verify that.
This method returns a collection of MountInfo objects, one for each mount :

Run the following code in your notebook to verify: 
```
mount  =dbutils.fs.mounts()
for mount in mounts:
  print(mount.mountPoint + " >> " + mount.source)

print("-"*80)

```
#### dbutils.fs.ls(..)
- And now we can use `dbutils.fs.ls(..)` to view the contents of that mount
- This method returns a collection of `FileInfo` objects, one for each item in the specified directory:

Run the following code in your notebook to view the contents: 

```
files = dbutils.fs.ls("/mnt/training/")

for fileInfo in files:
  print(fileInfo.path)

print("-"*80)
```

## Lab 2: Accessing and using data
### Section 1: Reading CSV
Using the instructions above, create a new Python notebook in the `training` folder in your Databricks workspace. Attach it to the cluster you have provisioned. Once you are in your notebook, run the below command to set up your environment:

`%run "./Includes/Classroom-Setup"`

#### Step 1: Entry Points
Our entry point for Spark 2.0 applications is the class SparkSession.

An instance of this object is already instantiated for us which can be easily demonstrated by running the below command in your notebook:
`print(spark)`

It's worth noting that in Spark 2.0 SparkSession is a replacement for the other entry points:

`SparkContext`, available in our notebook as *sc*.
`SQLContext`, or more specifically it's subclass `HiveContext`, available in our notebook as *sqlContext*.

Before we can dig into the functionality of the `SparkSession` class, we need to know how to access the API documentation for Apache Spark. Open a new browser tab, search for *Spark API Latest* or *Spark API x.x.x" for a specific version. The documentation you will use depends on which language you are using. 
You can access the [latest Spark Version documentation here](https://spark.apache.org/docs/latest/api.html), then click on the language you are working with. 

In this example, we are using Python so select *Spark Python API (Sphinx)*, then search the page for *pyspark.sql.SparkSession*. (You can also use the search bar on the left hand side of the page).

The function we are most interested in is `SparkSession.read()` which returns a `DataFrameReader`.

#### Step 2: Reading data
We are going to start by reading in a very simple text file, using the InferSchema flag. For this exercise, we will be using a tab-separated file called `pageviews_by_second.tsv` (255 MB file from Wikipedia). This is available in your Databricks workspace from the mounted `training` datastore. 

We can use `%fs ls ...` to view the file on the DBFS. Run the below command in a new cell in your notebook:

`%fs ls /mnt/training/wikipedia/pageviews/`

We can use `%fs head ...` to peek at the first couple thousand characters of the file.

`%fs head /mnt/training/wikipedia/pageviews/pageviews_by_second.tsv`

There are a couple of things to note here:
- The file has a header.
- The file is tab separated (we can infer that from the file extension and the lack of other characters between each "column").
- The first two columns are strings and the third is a number.
Knowing those details, we can read in the "CSV" file.

Let's start with the bare minimum by specifying the tab character as the delimiter and the location of the file:

```
# A reference to our tab-separated-file
csvFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

tempDF = (spark.read           # The DataFrameReader
   .option("sep", "\t")        # Use tab delimiter (default is comma-separator)
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)
```

This is guaranteed to <u>trigger one job</u>. A *Job* is triggered anytime we are "physically" __required to touch the data__.

In some cases, __one action may create multiple jobs__ (multiple reasons to touch the data). In this case, the reader has to __"peek" at the first line__ of the file to determine how many columns of data we have.

We can see the structure of the `DataFrame` by executing the command `printSchema()`. It prints to the console the name of each column, its data type and if it's null or not. Run the below command:

`tempDF.printSchema()`

We can see from the schema that...
* there are three columns
* the column names **_c0**, **_c1**, and **_c2** (automatically generated names)
* all three columns are **strings**
* all three columns are **nullable**

And if we take a quick peek at the data, we can see that line #1 contains the headers and not data - take a look at the dataframe with the below command:

`display(tempDF)`


#### Step 3: Use the file's Header
Next, we can add an option that tells the reader that the data contains a header and to use that header to determine our column names.

** *NOTE:* ** *We know we have a header based on what we can see in "head" of the file from earlier.*

Run the below command in your notebook:

```
(spark.read                    # The DataFrameReader
   .option("sep", "\t")        # Use tab delimiter (default is comma-separator)
   .option("header", "true")   # Use first line of all files as header
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
   .printSchema()
)
```

A couple of notes about this iteration:
* again, only one job
* there are three columns
* all three columns are **strings**
* all three columns are **nullable**
* the column names are specified: **timestamp**, **site**, and **requests** (the change we were looking for)

A "peek" at the first line of the file is all that the reader needs to determine the number of columns and the name of each column.

Before going on, make a note of the duration of the previous call - it should be just under 3 seconds.

#### Step 4: Infer the Schema

Lastly, we can add an option that tells the reader to infer each column's data type (aka the schema). Add `.option("inferSchema", "true")`  to your `spark.read` command. You should now have code like below:

```
(spark.read                        # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("sep", "\t")            # Use tab delimiter (default is comma-separator)
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(csvFile)                   # Creates a DataFrame from CSV after reading in the file
   .printSchema()
)
```
Looking at the results:
* we still have three columns
* all three columns are still **nullable**
* all three columns have their proper names
* two jobs were executed (not one as in the previous example)
* our three columns now have distinct data types:
  * **timestamp** == **timestamp**
  * **site** == **string**
  * **requests** == **integer**

#### Step 5: User defined Schema
This time we are going to read the same file, but define the schema beforehand to avoid the execution of any extra jobs. First, declare the schema for the file using the below code:
```
# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

csvSchema = StructType([
  StructField("timestamp", StringType(), False),
  StructField("site", StringType(), False),
  StructField("requests", IntegerType(), False)
])
```
Then, read the data and print the schema once more, specifying the schema, or rather the `StructType`, with the `schema(..)` command. Add the `.schema(csvSchema)` option to your existing command - it should now look as below:

```
(spark.read                   # The DataFrameReader
  .option('header', 'true')   # Ignore line #1 - it's a header
  .option('sep', "\t")        # Use tab delimiter (default is comma-separator)
  .schema(csvSchema)          # Use the specified schema
  .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
  .printSchema()
)
```

So let's review:
* We still have three columns
* All three columns are **NOT** nullable because we declared them as such.
* All three columns have their proper names
* Zero jobs were executed
* Our three columns now have distinct data types:
  * **timestamp** == **string**
  * **site** == **string**
  * **requests** == **integer**

For a list of all the options related to reading CSV files, please see the documentation for `DataFrameReader.csv(..)`

### Section 2: Reading Parquet
Apache Parquet is a columnar storage format available to any project in the Hadoop ecosystem, regardless of the choice of data processing framework, data model or programming language. Why use parquet? Well:
- Free & Open Source.
- Increased query performance over row-based data stores.
- Provides efficient data compression.
- Designed for performance on large data sets.
- Supports limited schema evolution.
- Is a splittable "file format".
- A [Column-Oriented data store](https://en.wikipedia.org/wiki/Column-oriented_DBMS)

The data for this example shows the number of requests to Wikipedia's mobile and desktop websites (23 MB from Wikipedia).

The original file, captured August 5th of 2016 was downloaded, converted to a Parquet file and made available for us at `/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/`. Take a look with `%fs ls..`:

`%fs ls /mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/`

Unlike our CSV example, the parquet "file" is actually 11 files, 8 of which consist of the bulk of the data and the other three consist of metadata.

#### Step 1: Read in the data
To read in this files, we will specify the location of the parquet directory, and use `spark.read.parquet(...)`

```
parquetFile = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

(parquetDF = spark.read              # The DataFrameReader
  .parquet(parquetFile)  # Creates a DataFrame from Parquet after reading in the file
  .printSchema()         # Print the DataFrame's schema
)
```

Notice that we do not need to specify the schema - the column names and data types are stored in the parquet files. Only one job is required to read that schema from the parquet file's metadata, and unlike the CSV reader that has to load the entire file and then infer the schema, the parquet reader can "read" the schema very quickly because it's *reading that schema from the metadata*.

If you want to avoid the extra job entirely, we can, again, specify the schema even for parquet files.

```
# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

parquetSchema = StructType(
  [
    StructField("timestamp", StringType(), False),
    StructField("site", StringType(), False),
    StructField("requests", IntegerType(), False)
  ]
)

(parquetDF = spark.read               # The DataFrameReader
  .schema(parquetSchema)  # Use the specified schema
  .parquet(parquetFile)   # Creates a DataFrame from Parquet after reading in the file
  )
```
In most/many cases, people do not provide the schema for Parquet files because reading in the schema is such a cheap process.

Lastly, let's peek at the data:

`display(parquetDF)`

### Section 3: Tables and Views
So far we've seen purely programmatic methods for reading in data. Databricks allows us to "register" the equivalent of "tables" so that they can be easily accessed by all users. It also allows us to specify configuration settings such as secret keys, tokens, username & passwords, etc without exposing that information to all users.

It is possible to upload data and create a table using it in the UI - instructions for doing this are available [in the documentation](https://docs.databricks.com/data/tables.html#create-a-table-using-the-ui). In this section, we are going to focus on using the `DataFrame` we already have and registering that as a view, exposing it as a table to the SQL API.

If you recall from earlier, we have an instance called `parquetDF`.

We can create a [temporary] view with this call:

```
# create a temporary view from the resulting DataFrame
parquetDF.createOrReplaceTempView("parquet_table")
```

And now we can use the SQL API to reference that same `DataFrame` as the table **parquet_table**.

```
%sql
select * from parquet_table order by requests desc limit(5)
```

The method `createOrReplaceTempView(..)` is bound to the SparkSession meaning it will be discarded once the session ends, it it can be a useful way to register temporary instances of your tables as you are developing your engineering pipelines. 

To create a table in databricks from your dataframe, use the `.write.saveAsTable("<table-name>")` function on your dataframe:

`parquetDF.write.saveAsTable("parquet_table")`

Now, when you click "data" on the side bar, you will see the table - click on the details view to show the table schema and sample data.

#### Managed vs Unmanaged Tables
Spark allows you to create two types of tables: managed and unmanaged. For a managed table, Spark manages both the metadata and the data in the file store. This could be a local filesystem, HDFS, or an object store such as Azure Blob. For an unmanaged table, Spark only manages the metadata, while you manage the data yourself in an external data source such as Cassandra.

With a managed table, because Spark manages everything, a SQL command such as DROP TABLE table_name *deletes both the metadata and the data*. With an unmanaged table, the same command will delete only the metadata, not the actual data.

This is not important in this example, as you do not have write access to the training storage accounts - but this is something to bear in mind when working with your own data.

To create an unmanaged table using the same dataframe, you can use the below command:

```
(parquetDF
  .write
  .option("path", "YOURSTORAGEPATH")
  .saveAsTable("parquet-demo"))
```

#### Section 4: Column operations
We will continue using the same data source as above. Lets start by taking a look at the number of records in the dataframe:

```
total = parquetDF.count()

print("Record Count: {0:,}".format( total ))
```

And another peek at our data...

`display(parquetDF)`

As we view the data, we can see that there is no real rhyme or reason as to how the data is sorted.
* We cannot even tell if the column **project** is sorted - we are seeing only the first 1,000 of some 2.3 million records.
* The column **article** is not sorted as evident by the article **A_Little_Boy_Lost** appearing between a bunch of articles starting with numbers and symbols.
* The column **requests** is clearly not sorted.
* And our **bytes_served** contains nothing but zeros.

So let's start by sorting our data. In doing this, we can answer the following question:

*What are the top 10 most requested articles?*

#### `orderBy(..)` and `sort(..)`

If you look at the API docs, `orderBy(..)` is described like this:
> Returns a new Dataset sorted by the given expressions.

Both `orderBy(..)` and `sort(..)` arrange all the records in the `DataFrame` as specified.
* Like `distinct()` and `dropDuplicates()`, `sort(..)` and `orderBy(..)` are aliases for each other.
  * `sort(..)` appealing to functional programmers.
  * `orderBy(..)` appealing to developers with an SQL background.
* Like `orderBy(..)` there are two variants of these two methods:
  * `orderBy(Column)`
  * `orderBy(String)`
  * `sort(Column)`
  * `sort(String)`

All we need to do now is sort our previous `DataFrame`.

```
sortedDF = (parquetDF
  .orderBy("requests")
)
sortedDF.show(10, False)
```

As you can see, we are not sorting correctly - we need to reverse the sort. One might conclude that we could make a call like this:

`parquetDF.orderBy("requests desc")`

Try it in your notebook.

Why does this not work?
* The `DataFrames` API is built upon an SQL engine.
* There is a lot of familiarity with this API and SQL syntax in general.
* The problem is that `orderBy(..)` *expects the name of the column.*
* What we specified was an SQL expression in the form of **requests desc**.
* What we need is a way to programmatically express such an expression.
* This leads us to the second variant, `orderBy(Column)` and more specifically, the class `Column`.

** *Note:* ** *Some of the calls in the `DataFrames` API actually accept SQL expressions.*<br/>
*While these functions will appear in the docs as `someFunc(String)` it's very*<br>
*important to thoroughly read and understand what the parameter actually represents.*

#### The Column Class

The `Column` class is an object that encompasses more than just the name of the column, but also column-level-transformations, such as sorting in a descending order.

The first question to ask is how do I create a `Column` object?

In Python we have these options:
```
%python

# Scala & Python both support accessing a column from a known DataFrame
# Uncomment this if you are using the Python version of this notebook
# columnA = parquetDF["requests"]

# The $"column-name" version that works for Scala does not work in Python
# columnB = $"requests"      

# If we import ...sql.functions, we get a couple of more options:
from pyspark.sql.functions import *

# This uses the col(..) function
columnC = col("requests")

# This uses the expr(..) function which parses an SQL Expression
columnD = expr("a + 1")

# This uses the lit(..) to create a literal (constant) value.
columnE = lit("abc")

# Print the type of each attribute
print("columnC: {}".format(columnC))
print("columnD: {}".format(columnD))
print("columnE: {}".format(columnE))
```

In Scala we have these options:

```
%scala

// Scala & Python both support accessing a column from a known DataFrame
// Uncomment this if you are using the Scala version of this notebook
// val columnA = parquetDF("requests")    

// This option is Scala specific, but is arugably the cleanest and easy to read.
val columnB = $"requests"          

// If we import ...sql.functions, we get a couple of more options:
import org.apache.spark.sql.functions._

// This uses the col(..) function
val columnC = col("requests")

// This uses the expr(..) function which parses an SQL Expression
val columnD = expr("a + 1")

// This uses the lit(..) to create a literal (constant) value.
val columnE = lit("abc")
```
In the case of Python, the cleanest version is the **col("column-name")** variant.

In the case of Scala, the cleanest version is the **$"column-name"** variant.

So with that, we can now create a `Column` object, and apply the `desc()` operation to it:

```
column = col("requests").desc()

# Print the column type
print("column:", column)
```

So now we can bring these together:

```
sortedDescDF = (parquetDF
  .orderBy( col("requests").desc() )
)  
sortedDescDF.show(10, False) # The top 10 is good enough for now
```

It should be of no surprise that the **Main_Page** (in both the Wikipedia and Wikimedia projects) is the most requested page.

Followed shortly after that is **Special:Search**, Wikipedia's search page.

And if you consider that this data was captured in the August before the 2016 presidential election, the Trumps will be one of the most requested pages on Wikipedia.

## Lab 3: Machine Learning
Cleaning data and adding features creates the inputs for machine learning models, which are only as strong as the data they are fed. This lab examines the process of featurization including common tasks such as handling categorical features and normalization, imputing missing data, and creating a pipeline of featurization steps.

### Concepts

#### Transformers, Estimators, and Pipelines

Spark's machine learning library, `MLlib`, has three main abstractions:<br><br>

1. A **transformer** takes a DataFrame as an input and returns a new DataFrame with one or more columns appended to it.  
  - Transformers implement a `.transform()` method.  
2. An **estimator** takes a DataFrame as an input and returns a model, which itself is a transformer.
  - Estimators implements a `.fit()` method.
3. A **pipeline** combines together transformers and estimators to make it easier to combine multiple algorithms.
  - Pipelines implement a `.fit()` method.

These basic building blocks form the machine learning process in Spark from featurization through model training and deployment.  

Machine learning models are only as strong as the data they see and can only work on numerical data.  **Featurization is the process of creating this input data for a model.**  There are a number of common featurization approaches:<br><br>

* Encoding categorical variables
* Normalizing
* Creating new features
* Handling missing values
* Binning/discretizing

This lesson builds a pipeline of transformers and estimators in order to featurize a dataset.

<div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/pipeline.jpg" style="height: 400px; margin: 20px"/></div>

#### Categorical Features and One-Hot Encoding

Categorical features refer to a discrete number of groups.  In the case of the AirBnB dataset we'll use in this lesson, one categorical variable is room type.  There are three types of rooms: `Private room`, `Entire home/apt`, and `Shared room`.

A machine learning model does not know how to handle these room types.  Instead, we must first *encode* each unique string into a number.  Second, we must *one-hot encode* each of those values to a location in an array.  This allows our machine learning algorithms to model effects of each category.

| Room type       | Room type index | One-hot encoded room type index |
|-----------------|-----------------|---------------------------------|
| Private room    | 0               | [1, 0 ]                         |
| Entire home/apt | 1               | [0, 1]                          |
| Shared room     | 2               | [0, 0]                          |


### Step 1: Reading and preparing data
#### Reading data
Import the AirBnB dataset.

`airbnbDF = spark.read.parquet("/mnt/training/airbnb/sf-listings/sf-listings-correct-types.parquet")`

`display(airbnbDF)`

Take the unique values of `room_type` and index them to a numerical value.  Fit the `StringIndexer` estimator to the unique room types using the `.fit()` method and by passing in the data.

The trained `StringIndexer` model then becomes a transformer.  Use it to transform the results using the `.transform()` method and by passing in the data.

```
from pyspark.ml.feature import StringIndexer

uniqueTypesDF = airbnbDF.select("room_type").distinct() # Use distinct values to demonstrate how StringIndexer works

indexer = StringIndexer(inputCol="room_type", outputCol="room_type_index") # Set input column and new output column
indexerModel = indexer.fit(uniqueTypesDF)                                  # Fit the indexer to learn room type/index pairs
indexedDF = indexerModel.transform(uniqueTypesDF)                          # Append a new column with the index

display(indexedDF)
```

Now each room has a unique numerical value assigned.  While we could pass the new `room_type_index` into a machine learning model, it would assume that `Shared room` is twice as much as `Entire home/apt`, which is not the case.  Instead, we need to change these values to a binary yes/no value if a listing is for a shared room, entire home, or private room.

Do this by training and fitting the `OneHotEncoder`, which only operates on numerical values (this is why we needed to use `StringIndexer` first).

NB: Certain models, such as random forest, do not need one-hot encoding (and can actually be negatively affected by the process).  The models we'll explore in this Lab, however, do need this process.

```
from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(inputCols=["room_type_index"], outputCols=["encoded_room_type"])
encoderModel = encoder.fit(indexedDF)
encodedDF = encoderModel.transform(indexedDF)

display(encodedDF)
```

The new column `encoded_room_type` is a vector.  The difference between a sparse and dense vector is whether Spark records all of the empty values.  In a sparse vector, like we see here, Spark saves space by only recording the places where the vector has a non-zero value.  The value of 0 in the first position indicates that it's a sparse vector.  The second value indicates the length of the vector.

Here's how to read the mapping above:<br><br>

* `Shared room` maps to the vector `[0, 0]`
* `Entire home/apt` maps to the vector `[0, 1]`
* `Private room` maps to the vector `[1, 0]`

#### Imputing Null or Missing Data

Null values refer to unknown or missing data as well as irrelevant responses. Strategies for dealing with this scenario include:<br><br>

* **Dropping these records:** Works when you do not need to use the information for downstream workloads
* **Adding a placeholder (e.g. `-1`):** Allows you to see missing data later on without violating a schema
* **Basic imputing:** Allows you to have a "best guess" of what the data could have been, often by using the mean of non-missing data
* **Advanced imputing:** Determines the "best guess" of what data should be using more advanced strategies such as clustering machine learning algorithms or oversampling techniques <a href="https://jair.org/index.php/jair/article/view/10302" target="_blank">such as SMOTE.</a>

<img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Try to determine why a value is null.  This can provide information that can be helpful to the model.

Describe the dataset and take a look at the `count` values.  There's a fair amount of missing data in this dataset.

`display(airbnbDF.describe())`

Try dropping missing values.

```
countWithoutDropping = airbnbDF.count()
countWithDropping = airbnbDF.na.drop(subset=["zipcode", "host_is_superhost"]).count()

print("Count without dropping nulls:\t", countWithoutDropping)
print("Count with dropping nulls:\t", countWithDropping)
```

Another common option for working with missing data is to impute the missing values with a best guess for their value.  Try imputing a list of columns with their median.

```
from pyspark.ml.feature import Imputer

imputeCols = [
  "host_total_listings_count",
  "bathrooms",
  "beds", 
  "review_scores_rating",
  "review_scores_accuracy",
  "review_scores_cleanliness",
  "review_scores_checkin",
  "review_scores_communication",
  "review_scores_location",
  "review_scores_value"
]

imputer = Imputer(strategy="median", inputCols=imputeCols, outputCols=imputeCols)
imputerModel = imputer.fit(airbnbDF)
imputedDF = imputerModel.transform(airbnbDF)

display(imputedDF)
```

#### Step 2: Creating a Pipeline

Passing around estimator objects, trained estimators, and transformed dataframes quickly becomes cumbersome.  Spark uses the convention established by `scikit-learn` to combine each of these steps into a single pipeline.
We can now combine all of these steps into a single pipeline.

```
from pyspark.ml import Pipeline

pipeline = Pipeline(stages=[
  indexer, 
  encoder, 
  imputer
])
```

The pipeline is itself is now an estimator.  Train the model with its `.fit()` method and then transform the original dataset.  We've now combined all of our featurization steps into one pipeline with three stages.

```
pipelineModel = pipeline.fit(airbnbDF)
transformedDF = pipelineModel.transform(airbnbDF)

display(transformedDF)
```
#### Step 3: Regression Modeling Theory

Linear regression is the most commonly employed machine learning model since it is highly interpretable and well studied.  This is often the first pass for data scientists modeling continuous variables.  This lesson trains simple and multivariate regression models and interprets the results.

##### Lines through Data

Take the example of housing data where we have median value for a number of neighborhoods and variables such as the number of rooms, per capita crime, and economic status of residents.  We might have a number of questions about this data including:<br><br>

1. *Is there a relationship* between our features and median home value?
2. If there is a relationship, *how strong is that relationship?*
3. *Which of the features* affect median home value?
4. *How accurately can we estimate* the effect of each feature on home value?
5. *How accurately can we predict* on unseen data?
6. Is the relationship between our features and home value *linear*?
7. Are there *interaction effects* (e.g. value goes up when an area is not industrial and has more rooms on average) between the features?

Generally speaking, machine learning models either allow us to infer something about our data or create accurate predictions.  **There is a trade-off between model accuracy and interpretability.**  More complex models generally perform better, which increases their accuracy at the expense of their interpretability.  

Linear regression is a highly interpretable model, allowing us to infer the answers to the questions above.  The predictive power of this model is somewhat limited, however, so if we're concerned about how our model will work on unseen data, we might choose a different model.

<div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/rm-vs-mdv.png" style="height: 600px; margin: 20px"/></div>

At a high level, **linear regression can be thought of as lines put through data.**  The line plotted above uses a linear regression model to create a best guess for the relationship between average number of rooms in a home and home value.  

##### Simple Linear Regression

Simple linear regression looks to predict a response `Y` using a single input variable `X`.  In the case of the image above, we're predicting median home value, or `Y`, based on the average number of rooms.  More technically, linear regression is estimating the following equation:

&nbsp;&nbsp;&nbsp;&nbsp;`Y ≈ β<sub>0</sub> + β<sub>1</sub>X`

In this case, `β<sub>0</sub>` and `β<sub>1</sub>` are our **coefficients** where `β<sub>0</sub>` represents the line's intercept with the Y axis and `β<sub>1</sub>` represents the number we multiply by X in order to attain a prediction.  **A simple linear regression model will try to fit our data a closely as possible by estimating these coefficients,** putting a line through the data.

<img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In the case of inferential statistics where we're interested in learning about the relationship between our input features and outputs, it's common to skip the train/test split step, as you'll see in this lesson.

#### Step 4 : Import and prepare data
Import the Boston dataset.

```
bostonDF = (spark.read
  .option("HEADER", True)
  .option("inferSchema", True)
  .csv("/mnt/training/bostonhousing/bostonhousing/bostonhousing.csv")
  .drop("_c0")
)

display(bostonDF)
```

Create a column `features` that has a single input variable `rm` by using `VectorAssembler`

```
from pyspark.ml.feature import VectorAssembler

featureCol = ["rm"]
assembler = VectorAssembler(inputCols=featureCol, outputCol="features")

bostonFeaturizedDF = assembler.transform(bostonDF)

display(bostonFeaturizedDF)
```

#### Step 5: Fit model

Fit a linear regression model. See the <a href="http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=vectorassembler#pyspark.ml.regression.LinearRegression" target="_blank">LinearRegression</a> documentation for more details.

```
from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol="features", labelCol="medv")

lrModel = lr.fit(bostonFeaturizedDF)
```

#### Step 6: Model Interpretation

Interpreting a linear model entails answering a number of questions:<br><br>

1. What did the model estimate my coefficients to be?
2. Are my coefficients statistically significant?
3. How accurate was my model?

Recalling that our model looks like `Y ≈ β<sub>0</sub> + β<sub>1</sub>X`, take a look at the model.

```
print("β0 (intercept): {}".format(lrModel.intercept))
print("β1 (coefficient for rm): {}".format(*lrModel.coefficients))
```

For a 5 bedroom home, our model would predict `-35.7 + (9.1 * 5)` or `$18,900`.  That's not too bad.

<img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The intercept of `-34.7` doesn't make a lot of sense on its own since this would imply that a studio apartment would be worth negative dollars.  Also, we don't have any 1 or 2 bedroom homes in our dataset, so the model will perform poorly on data in this range.

In order to determine whether our coefficients are statistically significant, we need to quantify the likelihood of seeing the association by chance.  One way of doing this is using a p-value.  As a general rule of thumb, a p-value of under .05 indicates statistical significance in that there is less than a 1 in 20 chance of seeing the correlation by mere chance.

Do this using the `summary` attribute of `lrModel`.

<img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The t-statistic can be used instead of p-values.  <a href="https://en.wikipedia.org/wiki/P-value" target="_blank">Read more about p-values here.</a>

```
summary = lrModel.summary

summary.pValues
```

These small p-values indicate that it is highly unlikely to see the correlation of the number of rooms to housing price by chance.  The first value in the list is the p-value for the `rm` feature and the second is that for the intercept.

Finally, we need a way to quantify how accurate our model is.  **R<sup>2</sup> is a measure of the proportion of variance in the dataset explained by the model.**  With R<sup>2</sup>, a higher number is better.

Run the below code to view the R<sup>2</sup> value:

`summary.r2`

This indicates that 48% of the variability in home value can be explained using `rm` and the intercept.  While this isn't too high, it's not too bad considering that we're training a model using only one variable.

Finally, take a look at the `summary` attribute of `lrModel` so see other ways of summarizing model performance.

`[attr for attr in dir(summary) if attr[0] != "_"]`

#### Step 7: Multivariate Regression

While simple linear regression involves just a single input feature, multivariate regression takes an arbitrary number of input features.  The same principles apply that we explored in the simple regression example.  The equation for multivariate regression looks like the following where each feature `p` has its own coefficient:

&nbsp;&nbsp;&nbsp;&nbsp;`Y ≈ β<sub>0</sub> + β<sub>1</sub>X<sub>1</sub> + β<sub>2</sub>X<sub>2</sub> + ... + β<sub>p</sub>X<sub>p</sub>`

<img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Our ability to visually explain how our model is performing becomes more limited as our number of features go up since we can only intuitively visualize data in two, possibly three dimensions.  With multivariate regression, we're therefore still putting lines through data, but this is happening in a higher dimensional space.

Train a multivariate regression model using `rm`, `crim`, and `lstat` as the input features. First, assemble the features:

```
from pyspark.ml.feature import VectorAssembler

featureCols = ["rm", "crim", "lstat"]
assemblerMultivariate = VectorAssembler(inputCols=featureCols, outputCol="features")

bostonFeaturizedMultivariateDF = assemblerMultivariate.transform(bostonDF)

display(bostonFeaturizedMultivariateDF)
```

Train the model.

```
from pyspark.ml.regression import LinearRegression

lrMultivariate = (LinearRegression()
  .setLabelCol("medv")
  .setFeaturesCol("features")
)

lrModelMultivariate = lrMultivariate.fit(bostonFeaturizedMultivariateDF)

summaryMultivariate = lrModelMultivariate.summary
```

Take a look at the coefficients and R<sup>2</sup> score.

```
print("β0 (intercept): {}".format(lrModelMultivariate.intercept))
for i, (col, coef) in enumerate(zip(featureCols, lrModelMultivariate.coefficients)):
  print("β{} (coefficient for {}): {}".format(i+1, col, coef))
  
print("\nR2 score: {}".format(lrModelMultivariate.summary.r2))
```

Our R<sup>2</sup> score improved from 48% to 64%, indicating that our new model can explain more of the variance in the data.

Congratulations, you completed the labs!

## Acknowledgements
These labs are based on content made available as part of the Microsoft Learn Training Path for Azure Databricks.
- https://docs.microsoft.com/en-us/learn/paths/data-engineer-azure-databricks/
- https://github.com/solliancenet/microsoft-learning-paths-databricks-notebooks
