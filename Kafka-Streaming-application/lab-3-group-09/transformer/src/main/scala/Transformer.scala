import java.time.Duration
import java.util.Properties
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.processor.{Cancellable, Punctuator}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde
//import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier, PunctuationType}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.processor.To
import org.apache.kafka.streams.state.{KeyValueIterator, Stores}
//import scala.util.{Try, Success, Failure}

object Transformer extends App {
  import Serdes._

  //set stream property
  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "transformer")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "lab-3-group-09_kafka-server_1:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass) // key serdes type set to string. matches the stream key type
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass) // value serdes type set to string. matches the stream value type
    p
  }

  // define case class used for output stream and kvstore
  case class CityRefugeeChange(city_id:Long,city_name:String,refugees:Long,change:Long)
  // define case class used for input stream
  case class Event(timestamp:Long, city_id: Long, city_name: String, refugees: Long )

  // build a transformation between input topic "events" and output topic "updates
  // and adhere kvstore "refugee_store" to it
  val inputTopic="events"
  val outputTopic="updates"
  val builder: StreamsBuilder = new StreamsBuilder
  val input: KStream[String,String] = builder.stream[String,String]("events")//read from inputTopic

  val storeBuilder = Stores.keyValueStoreBuilder(
	  Stores.inMemoryKeyValueStore("refugee_store"),
	  Serdes.String,  // store key: city_id of type string
	  Serdes.String)  // store value: CityRefugeeChange in json string style
  builder.addStateStore(storeBuilder)

  val outputStream = input.transform(new TransformerSupplier[String,String,KeyValue[String,String]]() {
    override def get:Transformer[String,String,KeyValue[String,String]] = {
      val transformer = new refugeeCountTransformer
      return transformer.asInstanceOf[Transformer[String,String,KeyValue[String,String]]]
    }
  }, "refugee_store").to(outputTopic) //adhere the kvStore to transformer

  val streams = new KafkaStreams(builder.build(), props)
  streams.start()

  sys.ShutdownHookThread {
    val timeout = 10;
    streams.close(Duration.ofSeconds(timeout))
  }
  

  class refugeeCountTransformer extends Transformer[String, String,KeyValue[String,String]] {
    var kvStore:KeyValueStore[String,String] = _ // key:(city_id,city_name) value:refugees
    var context:ProcessorContext = _
    var count = 0
    var windowSize:Int = _

    // Initialize Transformer object
    def init(context: ProcessorContext) {
      this.context = context
      kvStore = context.getStateStore("refugee_store").asInstanceOf[KeyValueStore[String, String]]
      // the windowsize is initialized by the first command line argument

      // input check: input needs to be a postive integer, otherwise will be regarded as invalid and the kafka stream will shut down
      val input = Typecheck.matchFunc(args(0))
      if (input == 0 || input == -1) {
        println("******************************************************")
        println("Invalid input, program terminated")
        close()
      } else {
        println("******************************************************")
        println("        The windowSize will be " + input + " seconds   ")
      }
      windowSize = input

  }

    // transform input stream to output stream, and update kvstore
    def transform(key: String, record: String): KeyValue[String,String] = {
      val decodedEvent=decode[Event](record).getOrElse("error").asInstanceOf[Event]
      val city_id = decodedEvent.city_id
      val city_name = decodedEvent.city_name
      val refugees = decodedEvent.refugees
      var refugee_update: Long = 0
      var change:Long = 0

      // if it is the first arrival of this city
      if (kvStore.get(key) == null){
          refugee_update = refugees // the historical total amount of refugees should be initialized to the firstly arrived refugee num
          change = refugees         // the total amount of refugees inside time window should be initialized to the firstly arrived refugee num
         
      }

      // if the city has appeared before and is already in kvstore
      else{
        val state = decode[CityRefugeeChange](kvStore.get(key)).getOrElse("error").asInstanceOf[CityRefugeeChange]
        //val state = decode[CityRefugee](kvStore.get(key)).getOrElse("error").asInstanceOf[CityRefugee]
        //println(state)
        val add:Long = refugees  // num of the newly arrived refugees
        val old_sum:Long = state.refugees
        val old_windowed:Long = state.change // the total amount of refugees in the current timewindow for this city
        refugee_update = add + old_sum   // the newly arrived refugees will be added to the historical total amount of refugees for this city
        change = old_windowed + refugees // the newly arrived refugees will be added to the total amount of the refugees of this city inside window
       
      }

      kvStore.put(key,CityRefugeeChange(city_id,city_name,refugee_update,change).asJson.noSpaces) // update the kvstore

      // create a disposable punctuator for this newly arrived city
      // which will be scheduled only once in N (windowsize) seconds
      // this is for removing the out-of-date record from the timewindow
      val punctuation = new myPunctuation()
      // save current record info inside punctuator, for the convenience of further update of kvstore
      punctuation.setInfo(kvStore, city_id, refugees, context)
      val scheduled:Cancellable=context.schedule(Duration.ofSeconds(windowSize), PunctuationType.WALL_CLOCK_TIME, punctuation)
      punctuation.schedule=scheduled

      return (key,kvStore.get(key))  // write to output stream, of which the "change" field reflects the arrival of new record

    }

    // Close any resources if any
    def close() {
		val iter: KeyValueIterator[String, String] = kvStore.all() 
	 	while (iter.hasNext) { 
	 	kvStore.delete(iter.next().key) 
	 	} 
	 	iter.close()
    }

    // inner class of refugeeCountTransformer
    // after N seconds, the newly arrived record will be out-of-date and should be subtracted in "change" field in punctuate()
    class myPunctuation() extends Punctuator{
	    
      var schedule: Cancellable = _
      var kvStore:KeyValueStore[String,String] = _ // key:(city_id,city_name) value:refugees
      var context:ProcessorContext = _
      var tobe_removed_city:Long = _
      var tobe_removed_refugee:Long = _

      // this punctuate () method is for updating the change "field" of the city corresponds to the out-of-date record
      override def punctuate(timestamp: Long): Unit = {
        // retrieve the info of to-be-removed city
        val city_id = tobe_removed_city
        val state = decode[CityRefugeeChange](kvStore.get(city_id.toString)).getOrElse("error").asInstanceOf[CityRefugeeChange]
        val city_name = state.city_name
        val refugees = state.refugees
        val change = state.change
        val change_update = change - tobe_removed_refugee // the refugee num should be subtracted from the total refugee num inside timewindow
       
        kvStore.put(city_id.toString,CityRefugeeChange(city_id,city_name,refugees,change_update).asJson.noSpaces)
        context.forward(city_id.toString, kvStore.get(city_id.toString) , To.all().withTimestamp(1))
        // the punctuate() method is only used once and after that the schedule should be canceled
        schedule.cancel()
      }

      def setInfo(kvStore:KeyValueStore[String,String], city_id:Long, refugees: Long, context:ProcessorContext): Unit ={
        // save the info of current record, which will be removed from timewindow after N seconds
        this.kvStore = kvStore
        this.context = context
        tobe_removed_city = city_id
        tobe_removed_refugee = refugees

      }

    }
  }

}




