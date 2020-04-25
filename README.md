# Data Pipelines ETL in Redshift on AWS with Airflow 



## **Overview**
For this project, Data Pipelines ETL in Redshift on AWS with Airflow using Python was applied. The startup Sparkify wants to analyze the data they are collecting about music and user activity in its new music streaming app. We are currently collecting data in S3 Bucket and the json format and the analytics team is particularly interested in understanding what songs users are listening to. We create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.


## **Datasets**

**Song**
Sample Record :
```
{"num_songs": 1, "artist_id": "AR8IEZO1187B99055E", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Marc Shaiman", "song_id": "SOINLJW12A8C13314C", "title": "City Slickers", "duration": 149.86404, "year": 2008}
```

**Log**
Sample Record :
```
{"artist": "Sydney Youngblood", "auth": "Logged In", "firstName": "Jacob", "gender": "M", "itemInSession": 53, "lastName": "Klein", "length": 238.07955, "level": "paid", "location": "Tampa-St. Petersburg-Clearwater, FL", "method": "PUT", "page": "NextSong", "registration": 1.540558e+12, "sessionId": 954, "song": "Ain't No Sunshine", "status": 200, "ts": 1543449657796, "userAgent": ""Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2"", "userId": "73"}
```



## **Schema**

### Staging Tables

**staging_events** - events staging of table
```
artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId
```

**staging_songs** - song staging of table
```
song_id, num_songs, title, artist_name, artist_latitude, year, duration, artist_id, artist_longitude, artist_location
```

### Dimension Tables

**songs**  - songs of table
```
song_id, title, artist_id, year, duration
```

**artists**  - artists of table
```
artist_id, name, location, latitude, longitude
```

**time**  - time of table
```
start_time, hour, day, week, month, year, weekday
```

**users**  - users of table
```
user_id, first_name, last_name, gender, level
```

### Fact Table 

**songplays** - songplays of table
```
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```


## **AirFlow Tasks**

![Alt text](dag.png?raw=true "ETL Data Sparkify")



## **AirFlow Instructions**

**Add Airflow Connections**

1 - To go to the Airflow UI.

2 - Click on the **Admin** tab and select **Connections**.

3 - Under **Connections**, select **Create**.

4 - Create connection page, enter the following values:

**Connections AWS**

* **Conn Id**: Enter `aws_credentials`
* **Conn Type**: Enter `Amazon Web Services`
* **Login**: Enter your **Access key ID** from the IAM User credentials you downloaded earlier.
* **Password**: Enter your **Secret access key** from the IAM User credentials you downloaded earlier.

**Connections Redshift**

* **Conn Id**: Enter `redshift`
* **Conn Type**: Enter `Postgres`
* **Host**: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the **Clusters** page of the Amazon Redshift console.
* **Schema**: Enter your schema. Example: `sparkify`.
* **Login**: Enter your user. Example: `sparkifyuser`.
* **Password**: Enter your **Secret**.
* **Port**: Enter `5439`.


## Project Files

```README.md``` -> Project description. 

```dag.png``` -> Pipeline DAG image.

```airflow``` -> Airflow home.
