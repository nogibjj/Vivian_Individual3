"""
transform and load function
"""

from pyspark.sql import SparkSession

def load(dataset="dbfs:/FileStore/individual3/train.csv"):
    spark = SparkSession.builder.appName("Read_CSV").getOrCreate()
    # load csv and transform it by inferring schema 
    df = spark.read.csv(dataset, header=True, inferSchema=True)

    # transform into a delta lakes table and store it 
    df.write.format("delta").mode("overwrite").saveAsTable("titanic_delta")
    columns_to_drop = ["PassengerId", "Name", "Age",
                       "SibSp", "Parch", "Ticket",
                       "Fare", "Cabin", "Embarked"]
    df = df.drop(*columns_to_drop)
    print(df)
    num_rows = df.count()
    print(num_rows)
    
    return "finished transform and load"

if __name__ == "__main__":
    load()