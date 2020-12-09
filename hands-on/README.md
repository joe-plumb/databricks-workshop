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
1. Start a new notebook
1. Run the below command to set up your environment
`%run "./Includes/Classroom-Setup"`
### Step 1: Entry Points
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

### Step 2: Reading data
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


### Step 3: Use the file's Header
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

### Step 4: Infer the Schema

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

### Step 5: User defined Schema
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

## Lab 3: Machine Learning

## Acknowledgements
These labs are based on content made available as part of the Microsoft Learn Training Path for Azure Databricks.
- https://docs.microsoft.com/en-us/learn/paths/data-engineer-azure-databricks/
- https://github.com/solliancenet/microsoft-learning-paths-databricks-notebooks
