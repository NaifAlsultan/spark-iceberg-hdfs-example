
# Assignment: Recommendation Pipeline with Apache Spark and Iceberg

**Introduction**

In this assignment, you will design and deploy a complete recommendation pipeline using Apache Spark and Apache Iceberg on HDFS/S3. Remote job submission via Apache Livy is bounce. Candidates are expected to demonstrate a solid general understanding of Apache Spark and Iceberg, including core concepts, architecture.

---

#### 1. Dataset & Schema

* **Storage:** Iceberg table on HDFS/S3 named `interactions`
* Data should be populated randomly.
* **Schema:**

    * `user_id: STRING`
    * `item_id: STRING`
    * `timestamp: LONG` (epoch ms)
    * `rating: DOUBLE`
    * `category: STRING`


---

#### 2. Transformations

1. **Read from Iceberg**
   Load the `interactions` table via the Iceberg catalog.

2. **Timestamp Parsing**
   Add two derived columns:

    * `event_date` (yyyy-MM-dd)
    * `event_hour` (0–23)

3. **Category Weight UDF**

    * Write a Scala UDF `categoryWeight(cat: String): Double` that maps each `category` to a numeric priority (e.g., 5→“A”, 1→“E”).
    * Compute `weighted_rating = rating * categoryWeight(category)`.

4. **7-Day Rolling Window**
   For each interaction, define a sliding window over the previous seven days (inclusive) for that user and compute:

    * Total of `weighted_rating`
    * Count of interactions


---

#### 3. Aggregations & Recommendations

1. **Per-User Scoring**
   Group by (`user_id`, `item_id`) and compute `avg_weighted_rating`.

2. **Top-N Selection**
   For each `user_id`, select the top 5 `item_id` by `avg_weighted_rating`.

3. **Write Results**
   Save the recommendations to a new Iceberg table on HDFS/S3:

   ```
   my_catalog.default.recommendations
   ```

   in overwrite or append mode. Include appropriate partitioning (e.g., by `event_date` or ingestion date).

---

#### 4. Deployment & Infrastructure

1. **Docker Compose**
   Provide a `docker-compose.yml` that brings up:

    * A minimal Hadoop (HDFS)/S3 cluster
    * *(Bounce)* A Livy Server

2. **Job Submission Script**
   In `client/submit_job.scala`, implement logic to:

    * Package the Spark job into a JAR and launch it with `spark-submit` wrapper against the Hadoop cluster
    * *(Bounce)* Submit via Livy’s REST API, poll for completion, and report status
    * After completion, retrieve and display the Iceberg snapshot ID of the `recommendations` table using Iceberg’s Java/Scala API

---


#### 5. Deliverables

* **`infra/docker-compose.yml`** — HDFS/S3 (and Livy, if used)
* **`job/`** directory — Spark job source, build file (e.g., `build.sbt` )
* **`client/submit_job.scala`** — submission script that also prints the Iceberg snapshot ID
* **`README.md`** — setup instructions, submission examples, table locations

---
