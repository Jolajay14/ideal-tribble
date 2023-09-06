# ideal-tribble
Big data

Using the databricks platform, I analysed two csv files of clinical trial datasets and pharmaceutical companies

my client wished to gain further insight into clinical trials. I was tasked with answering the following questions
1. The number of studies in the dataset. I ensured that distinct studies were explicitly checked.
2. You should list all the types (as contained in the Type column) of studies in thedataset along with the frequencies of each type. These should be ordered frommost frequent to least frequent
3. The top 5 conditions (from Conditions) with their frequencies.
4. Find the 10 most common sponsors that are not pharmaceutical companies, alongwith the number of clinical trials they have sponsored. the Parent Company column contains allpossible pharmaceutical companies.
5. Plot number of completed studies each month in a given year – for the submission dataset, the year is 2021. You need to include your visualization as well as a tableof all the values you have plotted for each month

The qustions were answered using three different implementations: Spark SQL, PySpark RDD and DataFrame
