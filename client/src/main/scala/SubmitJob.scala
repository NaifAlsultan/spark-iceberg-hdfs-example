import sttp.client4.quick._
import sttp.model.StatusCode
import sttp.model.Uri
import ujson.Value

object SubmitJob {
  val LIVY_URL = "http://localhost:8998"
  val JAR_PATH = "/opt/livy/jars/job.jar"
  val ICEBERG_DEP = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.1"
  val HDFS_PATH = "hdfs://namenode:9000/warehouse"
  val ICEBERG_CATALOG = "my_catalog"
  val ICEBERG_TABLE = s"$ICEBERG_CATALOG.default.recommendations"
  val POLL_MS = 3000

  def main(args: Array[String]): Unit = {
    println("Submitting job...")
    submit()
    println("Successfully submitted job.")

    println("Starting session...")
    val sessionId = startSession()
    println(s"Session '$sessionId' is ready!")

    println(s"Getting snapshot ID of '$ICEBERG_TABLE' table...")
    val snapshotId = getSnapshotId(sessionId)
    println(snapshotId)
  }

  def startSession(): Int = {
    val conf = ujson.Obj(
      "spark.jars.packages" -> ICEBERG_DEP,
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      s"spark.sql.catalog.$ICEBERG_CATALOG" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$ICEBERG_CATALOG.type" -> "hadoop",
      s"spark.sql.catalog.$ICEBERG_CATALOG.warehouse" -> HDFS_PATH
    )
    val payload = ujson.Obj(
      "kind" -> "spark",
      "conf" -> conf
    )

    val response = quickRequest
      .post(uri"$LIVY_URL/sessions")
      .header("Content-Type", "application/json")
      .body(ujson.write(payload))
      .send()

    if (!response.isSuccess) {
      println("Failed to start session.")
      println(response.show())
      sys.exit()
    }

    val json = ujson.read(response.body)
    val sessionId = json("id").num.toInt

    poll(uri"$LIVY_URL/sessions/$sessionId/state", "idle")

    sessionId
  }

  def submit(): Value = {
    val conf = ujson.Obj(
      "spark.jars.packages" -> ICEBERG_DEP
    )
    val payload = ujson.Obj(
      "file" -> JAR_PATH,
      "conf" -> conf
    )

    val response = quickRequest
      .post(uri"$LIVY_URL/batches")
      .header("Content-Type", "application/json")
      .body(ujson.write(payload))
      .send()

    if (!response.isSuccess) {
      println("Submission failed.")
      println(response.show())
      sys.exit()
    }

    val json = ujson.read(response.body)
    val submissionId = json("id").num.toInt

    poll(uri"$LIVY_URL/batches/$submissionId/state", "success")
  }

  def getSnapshotId(sessionId: Int): String = {
    val code =
      s"""spark.sql("SELECT snapshot_id, committed_at FROM $ICEBERG_TABLE.snapshots ORDER BY committed_at DESC").show(false)"""
    val payload = ujson.Obj("code" -> code)

    val response = quickRequest
      .post(uri"$LIVY_URL/sessions/$sessionId/statements")
      .header("Content-Type", "application/json")
      .body(ujson.write(payload))
      .send()

    if (!response.isSuccess) {
      println("Failed to get snapshot ID.")
      println(response.show())
      sys.exit()
    }

    val json = ujson.read(response.body)
    val statementId = json("id").num.toInt

    val pollJson = poll(
      uri"$LIVY_URL/sessions/$sessionId/statements/$statementId",
      "available"
    )

    val output = pollJson("output").obj
    val data = output("data").obj
    val text = data("text/plain").str

    text
  }

  def poll(uri: Uri, desiredState: String): Value = {
    while (true) {
      Thread.sleep(POLL_MS)

      val response = quickRequest
        .get(uri)
        .send()

      if (!response.isSuccess) {
        println(s"Polling $uri failed.")
        println(response.show())
        sys.exit()
      }

      val json = ujson.read(response.body)
      val state = json("state").str

      state match {
        case "running" | "starting" | "waiting" => println(s"Polling $uri...")
        case n if n == desiredState => {
          return json
        }
        case unknown => {
          println(s"Polling $uri failed with '$unknown' state")
          sys.exit()
        }
      }
    }
    sys.exit()
  }
}
