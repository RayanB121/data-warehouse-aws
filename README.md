
# Sparkify S3 to Redshift ETL

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project is an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.




## Schema

Fact Table: songplays
| Column       | Data type                           | 
|--------------|---------------------------------------|
| songplay_id  | BIGINT                 |
| start_time   | TIMESTAMP            |
| user_id      | TEXT                        |
| level        | TEXT            |
| song_id      | TEXT                         |
| artist_id    |TEXT                    |
| session_id   | TEXT                          |
| location     | TEXT  |
| user_agent   | TEXT     |

 Dimension Tables:

 users
| Column       | Data type                           |
|--------------|---------------------------------------|
| user_id      | TEXT                        |
| first_name   |TEXT                 |
| last_name    | TEXT                 |
| gender       | CHAR(1)                   |
| level        | TEXT              |

 songs
| Column       | Data type                           |
|--------------|---------------------------------------|
| song_id      | TEXT                        |
| title        | TEXT                      |
| artist_id    | TEXT                     |
| year         | SMALLINT            |
| duration     | FLOAT4   |

 artists
| Column       | Data type                           |
|--------------|---------------------------------------|
| artist_id    | TEXT                      |
| name         | TEXT                     |
| location     | TEXT                 |
| latitude     | FLOAT4     |
| longitude    | FLOAT4   |

 time
| Column       | Data type                           |
|--------------|---------------------------------------|
| start_time   | TIMESTAMP             |
| hour         | SMALLINT                  |
| day          | SMALLINT                  |
| week         | SMALLINT                 |
| month        | SMALLINT                 |
| year         | SMALLINT                 |
| weekday      | SMALLINT |



## Runing

clone project

write aws credentials on dwh.cfg



```bash
  python create_table.py
  python etl.py
```
    