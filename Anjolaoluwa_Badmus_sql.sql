-- Databricks notebook source
-- DBTITLE 1,Creating sql tables and views from existing dataframe
-- MAGIC %python                             #list the contents of the directory 
-- MAGIC 
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/clinicaltrial_2021.csv")     #Check the contents of the clinicaltrial_2021.csv file

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_rdd = sc.textFile("/FileStore/tables/clinicaltrial_2021.csv")  # #define the main rdd by reading in the clinicaltrial_2021.csv file
-- MAGIC 
-- MAGIC #Transform the main rdd to make each piece of information a separate element in the rdd
-- MAGIC 
-- MAGIC clinicaltrial_rdd1 = clinicaltrial_rdd.map(lambda line: line.split("|")).map(lambda values: [values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], ""] if len(values) < 9 else [values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8]])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import *           #Import the necessary module to define the schema for the dataframe 
-- MAGIC 
-- MAGIC mySchema = \
-- MAGIC StructType([
-- MAGIC StructField ("Id", StringType()) ,                       #Define the schema for the dataframe 
-- MAGIC StructField ("Sponsor", StringType()) ,
-- MAGIC StructField ("Status", StringType()) ,
-- MAGIC StructField ("Start", StringType()) ,
-- MAGIC StructField ("Completion", StringType()) ,            #the dataframe contains 9 columns
-- MAGIC StructField ("Type", StringType()) ,
-- MAGIC StructField ("Submission", StringType()) ,
-- MAGIC StructField ("Conditions", StringType()) ,
-- MAGIC StructField ("Interventions", StringType())])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_df = spark.createDataFrame(clinicaltrial_rdd1, mySchema)    #create the dataframe by applying the defined schema to the rdd
-- MAGIC clinicaltrial_df.show()             #display the contents of the dataframe

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_df = spark.read.csv("/FileStore/tables/clinicaltrial_2021.csv", header=True, sep="|") #read the clinicaltrial file, specify the first row contains the header and the delimeter used is '|'
-- MAGIC 
-- MAGIC clinicaltrial_df.createOrReplaceTempView ("clinicaltrial_view") #create a temporary table(view) with the contents of the dataframe

-- COMMAND ----------

CREATE OR REPLACE TABLE default.clinicaltrial_2021 AS SELECT * FROM clinicaltrial_view   --create a new table clinicaltrial_2021 with the                                                                                             contents of the temporary table

-- COMMAND ----------

-- DBTITLE 1,Question 1
SELECT COUNT(DISTINCT Id) AS number_distinct_studies      --count number of distinct studies in the clinicaltrial_2021 table
FROM clinicaltrial_2021;

-- COMMAND ----------

-- DBTITLE 1,Question 2:
SELECT Type, COUNT(*) as count     ---list type of studies with their frequencies
    FROM clinicaltrial_2021
    WHERE Type != 'Type'
    GROUP BY Type
    ORDER BY count DESC;

-- COMMAND ----------

-- DBTITLE 1,Question 3:
SELECT Conditions, COUNT(*) AS Frequency
FROM (
  SELECT explode(split(Conditions, ',')) AS Conditions
  FROM clinicaltrial_2021
)
GROUP BY Conditions
ORDER BY Frequency DESC
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,Question 4: creating pharma table from pharm rdd
-- MAGIC %python
-- MAGIC from pyspark.sql.types import *
-- MAGIC 
-- MAGIC mySchema = StructType([
-- MAGIC     StructField("Company", StringType()),
-- MAGIC     StructField("Parent_Company", StringType()),
-- MAGIC     StructField("Penalty_Amount", StringType()),
-- MAGIC     StructField("Subtraction_From_Penalty", StringType()),
-- MAGIC     StructField("Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting", StringType())
-- MAGIC ])
-- MAGIC 
-- MAGIC pharma_rdd = sc.textFile("/FileStore/tables/pharma.csv") \
-- MAGIC              .map(lambda line: line.split(",")) \
-- MAGIC              .map(lambda values: (values[0], values[1], (values[2]), (values[3]), (values[4])))
-- MAGIC 
-- MAGIC pharma_df = spark.createDataFrame(pharma_rdd, mySchema)
-- MAGIC pharma_df.show(10)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma_df = spark.read.csv("/FileStore/tables/pharma.csv", header=True, sep=",") #read the pharma file, specify the first row contains the header and the delimeter used is ','
-- MAGIC 
-- MAGIC pharma_df.createOrReplaceTempView ("pharma_view") #create a temporary table(view)

-- COMMAND ----------

CREATE OR REPLACE TABLE default.pharma AS SELECT * FROM pharma_view   --create pharma table

-- COMMAND ----------

SELECT Sponsor, COUNT(*) AS Number_of_Trials
FROM clinicaltrial_2021
WHERE Sponsor NOT IN (SELECT DISTINCT Parent_Company FROM pharma)
GROUP BY Sponsor
ORDER BY Number_of_Trials DESC
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Question 5:
-- Extract month and year from Completion date, filter for completion dates with year 2021, and count number of studies for each month
SELECT CASE
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 1 THEN 'Jan'
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 2 THEN 'Feb'
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 3 THEN 'Mar'
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 4 THEN 'Apr'
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 5 THEN 'May'
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 6 THEN 'Jun'
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 7 THEN 'Jul'
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 8 THEN 'Aug'
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 9 THEN 'Sep'
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 10 THEN 'Oct'
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 11 THEN 'Nov'
         WHEN MONTH(TO_DATE(Completion, 'MMM yyyy')) = 12 THEN 'Dec'
       END AS Month,
       COUNT(*) AS NumberofCompletedStudies
FROM clinicaltrial_2021
WHERE Status = 'Completed' AND Completion LIKE '%2021%'
GROUP BY MONTH(TO_DATE(Completion, 'MMM yyyy'))
ORDER BY MONTH(TO_DATE(Completion, 'MMM yyyy'))

-- COMMAND ----------

-- DBTITLE 1,Further analysis 3:
-- Query to find the average duration of clinical trials by type in months, rounded to the nearest whole number
SELECT Type AS Type_of_Study, 
       ROUND(AVG(DATEDIFF(MONTH, to_date(Start, 'MMM yyyy'), to_date(Completion, 'MMM yyyy')))) AS Avg_duration_in_months
FROM clinicaltrial_2021
GROUP BY Type;

-- COMMAND ----------

-- DBTITLE 1,Visualization
-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC 
-- MAGIC # Data for the bar chart
-- MAGIC type_of_study = ['Observational [Patient Registry]', 'Expanded Access', 'Interventional', 'Observational']
-- MAGIC avg_duration = [59, 50, 34, 42]
-- MAGIC 
-- MAGIC # Create a bar chart
-- MAGIC plt.bar(type_of_study, avg_duration)
-- MAGIC 
-- MAGIC # Set the labels and title
-- MAGIC plt.xlabel('Type of Study')
-- MAGIC plt.ylabel('Average Duration (Months)')
-- MAGIC plt.title('Average Duration of Clinical Trials by Type of Study')
-- MAGIC 
-- MAGIC # Display the chart
-- MAGIC plt.show()

-- COMMAND ----------


