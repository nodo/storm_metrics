package uk.ac.ic.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

class MySpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Long _lastTimestamp;
    Random _rand;
    Integer _windowSizeSeconds = 20;
    Integer _differentMessages = 100;
    Integer _sizeInBytes = 1024;
    String [] _messages = null;

    transient CountMetric _countMetric;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _rand = new Random();
        _collector = spoutOutputCollector;
        _countMetric = new CountMetric();
        _lastTimestamp = System.currentTimeMillis();
        topologyContext.registerMetric("execute_count", _countMetric, _windowSizeSeconds);

        _messages = new String[_differentMessages];
        for(int i = 0; i < _differentMessages; i++) {
            StringBuilder sb = new StringBuilder(_sizeInBytes);
            for(int j = 0; j < _sizeInBytes; j++) {
                sb.append(_rand.nextInt(9));
            }
            _messages[i] = sb.toString();
        }
    }

    @Override
    public void nextTuple() {
        String message = _messages[_rand.nextInt(_messages.length)];
        _collector.emit(new Values(message));

        updateMetrics();
    }

    private void updateMetrics() {
        if (_lastTimestamp > _windowSizeSeconds * 1000) {
            _lastTimestamp = System.currentTimeMillis();
            _countMetric.getValueAndReset();
            _countMetric.incr();
        } else {
            _countMetric.incr();
        }
    }
}

class MyBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        _collector.emit(tuple, new Values(tuple.getString(0)));
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}

public class MetricsTopology {
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("my_spout", new MySpout());
        builder.setBolt("my_bolt", new MyBolt()).shuffleGrouping("my_spout");

        Config config = new Config();
        config.setDebug(true);
        config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);

        /* REMOTE DEPLOY
         * - create jar: Build -> Create Artifact
         * - submit topology to a Storm Cluster somewhere.
         *
         * LOCAL
         * - just run passing "local" as argument
         */
        if (args.length == 1 && args[0].equals("local")) {
            System.out.println( "Local mode");
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("MetricsTopology", config, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        } else {
            config.setNumWorkers(2);
            config.setNumAckers(2);
            StormSubmitter.submitTopologyWithProgressBar("MetricsTopology", config, builder.createTopology());
        }
    }
}
