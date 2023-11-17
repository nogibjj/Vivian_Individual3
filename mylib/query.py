# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np

def query():
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = (
        "SELECT Sex, AVG(Survived) as Survival "
        "FROM titanic_delta "
        "GROUP BY Sex "
    )
    query_result = spark.sql(query)
    return query_result

def viz():
    ## group by gender, calculate survival rate
    query_ = query().toPandas()
    # print(query_)
    plt.bar(query_.Sex, query_.Survival)
    plt.title("Survival Rate for Titanic Passengers by Gender")
    plt.xlabel("Gender")
    plt.ylabel("Survival Rate")
    plt.show()

    ## group by gender, survived
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query_2 = (
        "SELECT Sex, Survived, COUNT(*) as Count "
        "FROM titanic_delta "
        "GROUP BY Sex, Survived "
    )
    query2 = spark.sql(query_2).toPandas()
    sur = query2[query2["Survived"] == 1].Count.tolist()
    not_sur = query2[query2["Survived"] == 0].Count.tolist()
    ## making plot
    width = 0.35
    labels = ["Female", "Male"]
    x = np.arange(2)
    fig, ax = plt.subplots()
    bars1 = ax.bar(x - width / 2, not_sur, width, label="Not Survived")
    bars2 = ax.bar(x + width / 2, sur, width, label="Survived")
    print(bars1, bars2)
    ax.set_ylabel("Count")
    ax.set_title("Survival Count by Gender")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    plt.show()

if __name__ == "__main__":
    # print(type(query()))
    viz()