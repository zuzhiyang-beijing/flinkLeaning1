import org.apache.flink.api.common.functions.MapFunction

class ExtendFunction extends MapFunction[String,String]{

  override def map(t: String): String = {
    t.toUpperCase()
  }
}
