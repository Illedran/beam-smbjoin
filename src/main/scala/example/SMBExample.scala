package example

import com.spotify.scio._
/*
sbt "runMain [PACKAGE].WordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/


object SMBExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args.getOrElse("input","test.txt")
    val output = args.getOrElse("output", "bucketed")
    val numBuckets = args.getOrElse("numBuckets", "5").toInt
    val inputData = sc.textFile(input)
        .map(_.toInt)

    // TODO: figure out why implicit conversion is not working

    SMBSCollectionFunctions.makeSMBSCollectionFunctions(inputData)
        .sortMergeBuckets(numBuckets)

    val result = sc.close().waitUntilFinish()
  }
}


