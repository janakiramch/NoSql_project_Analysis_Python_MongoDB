# Please note that Barchart, PieChart, Scatterplot codes are referred from https://matplotlib.org and modified as per our usecase
# Basic code for MongoDB Database local connection
import pymongo as pymongo
from pymongo import MongoClient
import matplotlib.pyplot as plt

# Connecting to MongoDB by pymongo
client = MongoClient()
client = MongoClient("localhost", 27017)
mydatabase = client.project
collection = mydatabase.CS532

# Query-2(Part-1)
filter_online = {
    "$match": {
        "Use Chip": "Online Transaction" 
    }
}
filter_fraud = {
    "$match": {
        "Is Fraud?": "Yes"
    }
}
split_time = {
    "$project": {
        "MCC": 1,
        "Day": 1,
        "time_split": {
            "$split": [
                "$Time", ":"
            ]
        }, 
    }
}
project_time = {
    "$project": {
        "_id": 0,
        "MCC": 1,
        "Day": 1,
        "time_split": 1
        }
}

unwind_time = {
    "$unwind": {
        "path": "$time_split",
        "includeArrayIndex": "arrayIndex"
    }
}
include_array_0 = {
    "$match": {
        "arrayIndex": 0
    }
}

most_oocuring_month_time = {
                    "$group" : {
                        "_id" : {
                            "Day": "$Day",
                            "Time": "$time_split",
                        },
                        "Frequency" : {
                            "$sum" : 1.0
                        }
                    }
        }
sort_data = {
    "$sort" : {
        "Frequency" : -1.0
    }
}

#Pipeline for filtering dataset
pipeline = [
    filter_online,
    filter_fraud,
    split_time,
    project_time,
    unwind_time,
    include_array_0,
    most_oocuring_month_time,
    sort_data,
]

# Variables for plotting graphs
Result_dict = []
Day_time_dict = []
Day_array = []
time_array = []
freq_array = []
results = collection.aggregate(pipeline)
count = 0
for x in results:
    Result_dict.append(x)


for dct in Result_dict: 
    Day_time_dict.append(dct['_id'])
    freq_array.append(dct['Frequency'])


for dct in Day_time_dict:
    Day_array.append(dct['Day'])
    time_array.append(dct['Time'])


# Scatter Plot Implementation
scatter_time_var = [int(i) for i in time_array]
scatter_day_var = [int(i) for i in Day_array]
scatter_count_var = [int(i) for i in freq_array]
Main_scatter_time = scatter_time_var
Main_scatter_day = scatter_day_var
colors = scatter_count_var
plt.scatter(Main_scatter_time, Main_scatter_day, s=40, c=colors, cmap = "Reds")
plt.xlabel("Time of Fraud Transaction(24-hr Clock AM/PM)")
plt.ylabel("Day of Transaction")
plt.title("Fraud frequency of dataset")
cbar = plt.colorbar(orientation = "vertical")
cbar.set_label(label = "Fraud frequency", size=15)
plt.show()

# Query-2(Part-2)
filter_fraud_2 = {
    "$match": {
        "Is Fraud?": "Yes"
    }
}
filter_online = {
    "$match": {
        "Use Chip": "Online Transaction"
    }
}
filter_swipe = {
    "$match": {
        "Use Chip": "Swipe Transaction"
    }
}
filter_chip = {
    "$match": {
        "Use Chip": "Chip Transaction"
    }
}
pipeline1 = [
    filter_fraud_2,
    filter_online,
]
pipeline2 = [
    filter_fraud_2,
    filter_chip,
]
pipeline3 = [
    filter_fraud_2,
    filter_swipe,
]
count_online_fraud = 0
count_chip_fraud = 0
count_swipe_fraud = 0
results_online = collection.aggregate(pipeline1)
results_chip = collection.aggregate(pipeline2)
results_swipe = collection.aggregate(pipeline3)
for x in results_online:
    count_online_fraud = count_online_fraud + 1
for x in results_chip:
    count_chip_fraud = count_chip_fraud + 1
for x in results_swipe:
    count_swipe_fraud = count_swipe_fraud + 1
count_array = []
count_array.append(count_online_fraud)
count_array.append(count_chip_fraud)
count_array.append(count_swipe_fraud)
label = ["Online Frauds"," Chip Frauds", "Swipe Frauds" ]
pie_out = label
Data = count_array
plt.title("Frauds percentage in type of transaction")
plt.pie(Data, labels = pie_out, autopct = "%.1f")
plt.show() 






    

