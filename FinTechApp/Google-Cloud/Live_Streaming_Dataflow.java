package com.live_trading.app;

import java.io.IOException;
import java.io.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.*;
import java.util.function.Function;
//import com.sun.istack.internal.tools.DefaultAuthenticator.Receiver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.request.HttpRequestWithBody;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
//import com.mashape.unirest.http.exceptions.UnirestException.UnirestParsingException;
import com.mashape.unirest.http.async.Callback;
import org.json.JSONObject;
//import org.apache.beam.examples.common.ExampleOptions;
//import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.runners.dataflow.DataflowRunner;
//import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Read;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.readStrings;
//import org.apache.beam.sdk.io.gcp.pubsub;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSink;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.InvalidWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
//import org.apache.beam.sdk.transforms.Combine.globally.asSingletonView;
import org.apache.beam.sdk.transforms.SerializableFunction;
//import org.apache.beam.sdk.transforms.PTransform;
import org.joda.time.Duration;
import org.joda.time.Instant;
import java.lang.*;
import org.apache.commons.lang3.tuple.Pair;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.io.Serializable;
//import org.json.simple.JSONObject;
import avro.shaded.com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.runners.direct.WatermarkManager;
import org.apache.beam.runners.direct.Clock;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.joda.time.DateTime;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window.OnTimeBehavior;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import java.text.SimpleDateFormat;



public class App {
    public static class ParseInputsFn extends DoFn<String, String> {
    
      @ProcessElement
      public void processElement(@Element String element, OutputReceiver<String> receiver) {
        //HashMap<String,String> myMap = new HashMap<String,String>();
        //System.out.println("element");
        //System.out.println(element);
        ArrayList myList = new ArrayList();
        String[] pairs = element.split("\\s+");
        //System.out.println("pairs");
        //System.out.println(Arrays.toString(pairs));

        String Time_1 = pairs[9];
        //String TimeKey = "Time";
        myList.add(Time_1);

        String Symbol_1 = pairs[11];
        //String SymbolKey = "Symbol";
        myList.add(Symbol_1);

        String Price_1 = pairs[12];
        //String PriceKey = "Price";
        myList.add(Price_1);

        String Volume_1 = pairs[13];
        //String VolumeKey = "Volume";
        myList.add(Volume_1);

        String askPrice_1 = pairs[14];
        //String askPriceKey = "askPrice";
        myList.add(askPrice_1);

        String bidPrice_1 = pairs[15];
        //String bidPriceKey = "bidPrice";
        myList.add(bidPrice_1);

        String askSize_1 = pairs[16];
        //String askSizeKey = "askSize";
        myList.add(askSize_1);

        String bidSize_1 = pairs[16];
        //String bidSizeKey = "bidSize";
        myList.add(bidSize_1);

        String myListCommaSeparated = String.join(",", myList);

        receiver.output(myListCommaSeparated);
      }
    }

    public static class AddTimestampFn extends DoFn<String, String> {
      @ProcessElement
      public void processElement(@Element String element, OutputReceiver<String> receiver) {
        String[] element_1 = element.split(",");
        String randomTimestamp = element_1[0];
        Instant randomTimestamp_2 = new Instant((long)(Long.valueOf(randomTimestamp)/(long)1000000));
        receiver.outputWithTimestamp(element, randomTimestamp_2);
      }
    }

    public static class GroupBySymbolFn extends DoFn<String, KV<String, String>> {
  
      @ProcessElement
      public void processElement(@Element String element, OutputReceiver<KV<String, String>> receiver) {
        String[] element_1 = element.split(",");
        String symbol = element_1[1];
        List<KV<String, String>> outputElement_1 = Arrays.asList(KV.of(symbol, element));
        KV<String, String> outputElement_2 = outputElement_1.get(0);
        receiver.output(outputElement_2);
      }
    }

    public static class Combiner extends Combine.CombineFn<String, List<String>, String> {
 
      @Override
      public List<String> createAccumulator() {
        return new ArrayList<>();
      }
     
      @Override
      public List<String> addInput(List<String> accumulator, String input) {
        accumulator.add(input);
        Collections.sort(accumulator);
        return accumulator;
      }
     
      @Override
      public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
        List<String> mergedAccumulator = new ArrayList<>();
        accumulators.forEach(accumulatorToMerge -> mergedAccumulator.addAll(accumulatorToMerge));
     
        String valuesToMerge = Lists.newArrayList(accumulators).stream()
          .flatMap(listOfLetters -> {
            Collections.sort(listOfLetters);
            if (listOfLetters.isEmpty()) {
              return Stream.of("");
            } else {
              return Stream.of(String.join(",", listOfLetters));
            }
          })
          .sorted()
          .collect(Collectors.joining(","));
        //FanoutWithKeyResultHolder.INSTANCE.addValues(valuesToMerge);
        return mergedAccumulator;
      }
     
      @Override
      public String extractOutput(List<String> accumulator) {
        Collections.sort(accumulator);
        return String.join(",", accumulator);
      }
    }

    public static class FinalFn extends DoFn<KV<String, Iterable<String>>, String> {
    //public static class FinalFn extends DoFn<KV<String, String>, String> {
  
      @ProcessElement
      public void processElement(@Element KV<String, Iterable<String>> element, OutputReceiver<String> receiver) throws Exception {
      //public void processElement(@Element KV<String, String> element, OutputReceiver<String> receiver) throws Exception {

        Iterable<String> element_2 = element.getValue();
        ArrayList<ArrayList<String>> lists = new ArrayList<ArrayList<String>>();
        ArrayList<String> Time_list = new ArrayList<String>();
        ArrayList<String> Symbol_list = new ArrayList<String>();
        ArrayList<String> Price_list = new ArrayList<String>();
        ArrayList<String> Volume_list = new ArrayList<String>();
        ArrayList<String> askPrice_list = new ArrayList<String>();
        ArrayList<String> askSize_list = new ArrayList<String>();
        ArrayList<String> bidPrice_list = new ArrayList<String>();
        ArrayList<String> bidSize_list = new ArrayList<String>();
        for(String var_0: element_2) 
        { 
          //HashMap<String,String> var = element_2.get(i);
          String[] var = var_0.split(",");
          String Time_last = var[0];
          Time_list.add(Time_last);
          String Symbol_last = var[1];
          Symbol_list.add(Symbol_last);
          String Price_last = var[2];
          Price_list.add(Price_last);
          String Volume_last = var[3];
          Volume_list.add(Volume_last);
          String askPrice_last = var[4];
          askPrice_list.add(askPrice_last);
          String bidPrice_last = var[5];
          bidPrice_list.add(bidPrice_last);
          String askSize_last = var[6];
          askSize_list.add(askSize_last);
          String bidSize_last = var[7];
          bidSize_list.add(bidSize_last);
        }
        lists.add(Time_list);
        lists.add(Symbol_list);
        lists.add(Price_list);
        lists.add(Volume_list);
        lists.add(askPrice_list);
        lists.add(bidPrice_list);
        lists.add(askSize_list);
        lists.add(bidSize_list);
        String lists_2 = lists.toString();
        HttpResponse<JsonNode> jsonResponse = Unirest.post("https://*****.appspot.com/echo?key=*******").header("content-type", "application/json").body("{\"message\":\"" + lists_2 + "\"}").asJson();
        //HttpResponse<JsonNode> jsonResponse = Unirest.post("http://******/:8080/echo?key=******").header("content-type", "application/json").body("{\"message\":\"" + lists_2 + "\"}").asJson();
        String message_1 = jsonResponse.getBody().getObject().getString("message");
        if ((!"invalid trade".equals(message_1)==true) && (!"Exception".equals(message_1.substring(0, 9))==true)) {
          receiver.output(message_1);
        }

      }
    }

    public static class FinalFn_2 extends DoFn<String, String> {
  
      @ProcessElement
      public void processElement(@Element String element, OutputReceiver<String> receiver) throws Exception {

        String element_2 = element;
        ArrayList<ArrayList<String>> lists = new ArrayList<ArrayList<String>>();
        ArrayList<String> Time_list = new ArrayList<String>();
        ArrayList<String> Symbol_list = new ArrayList<String>();
        ArrayList<String> Price_list = new ArrayList<String>();
        ArrayList<String> Volume_list = new ArrayList<String>();
        ArrayList<String> askPrice_list = new ArrayList<String>();
        ArrayList<String> askSize_list = new ArrayList<String>();
        ArrayList<String> bidPrice_list = new ArrayList<String>();
        ArrayList<String> bidSize_list = new ArrayList<String>();
        String[] var = element_2.split(",");
        for (int i = 0; i < (var.length/8); i++) {
          String Time_last = var[(i*8)+0];
          Time_list.add(Time_last);
          String Symbol_last = var[(i*8)+1];
          Symbol_list.add(Symbol_last);
          String Price_last = var[(i*8)+2];
          Price_list.add(Price_last);
          String Volume_last = var[(i*8)+3];
          Volume_list.add(Volume_last);
          String askPrice_last = var[(i*8)+4];
          askPrice_list.add(askPrice_last);
          String bidPrice_last = var[(i*8)+5];
          bidPrice_list.add(bidPrice_last);
          String askSize_last = var[(i*8)+6];
          askSize_list.add(askSize_last);
          String bidSize_last = var[(i*8)+7];
          bidSize_list.add(bidSize_last);
        }
        lists.add(Time_list);
        lists.add(Symbol_list);
        lists.add(Price_list);
        lists.add(Volume_list);
        lists.add(askPrice_list);
        lists.add(bidPrice_list);
        lists.add(askSize_list);
        lists.add(bidSize_list);
        String lists_2 = lists.toString();
        HttpResponse<JsonNode> jsonResponse = Unirest.post("https://*****.appspot.com/echo?key=******").header("content-type", "application/json").body("{\"message\":\"" + lists_2 + "\"}").asJson();
        //HttpResponse<JsonNode> jsonResponse = Unirest.post("http://*******/:8080/echo?key=*******").header("content-type", "application/json").body("{\"message\":\"" + lists_2 + "\"}").asJson();
        String message_1 = jsonResponse.getBody().getObject().getString("message");
        if ((!"invalid trade".equals(message_1)==true) && (!"Exception".equals(message_1.substring(0, 9))==true)) {
          receiver.output(message_1);
        }

      }
    }

    public static void runWindowedTrading(PipelineOptions options) throws IOException {
      Pipeline pipeline = Pipeline.create(options);
      PCollection<String> Group_By_Symbol_1 =
          pipeline
              .apply("Read_Strings", PubsubIO.readStrings().fromSubscription("projects/*****/subscriptions/subscription_1").withTimestampAttribute("publish_time"))
              .apply(Window.<String>into(SlidingWindows.of(Duration.standardMinutes(2))).triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(1)).getContinuationTrigger()).getContinuationTrigger()).accumulatingFiredPanes().withAllowedLateness(Duration.millis(1), ClosingBehavior.valueOf("FIRE_IF_NON_EMPTY")).withTimestampCombiner(TimestampCombiner.valueOf("EARLIEST")).withOnTimeBehavior(Window.OnTimeBehavior.valueOf("FIRE_IF_NON_EMPTY")))
              .apply("Parse_Strings", ParDo.of(new ParseInputsFn()))
              .apply(Combine.globally(new Combiner()).withoutDefaults())
              .apply("Final", ParDo.of(new FinalFn_2()));
      try {
        Group_By_Symbol_1
        .apply("Write_Strings", PubsubIO.writeStrings().to("projects/*****/topics/topic_2").withMaxBatchSize(1));
      }
      catch (Exception e) {
          System.out.println(e);
      }

      pipeline.run().waitUntilFinish();
      //pipeline.run();
    }
    public static void main(String[] args) throws IOException {
    	DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
    	options.setProject("*****");
      options.setStagingLocation("gs://bucket-myalgo-1/staging/");
      options.setTempLocation("gs://bucket-myalgo-1/tmp/");
      options.setServiceAccount("*****-compute@developer.gserviceaccount.com");
      options.setRunner(DataflowRunner.class);
      options.setAutoscalingAlgorithm(DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.NONE);
      options.setNumWorkers(1);
      options.setWorkerMachineType("c2-standard-8");
      options.setZone("us-central1-c");
      options.setWorkerDiskType("compute.googleapis.com/projects//zones//diskTypes/pd-ssd");
      List<String> expNames_1 = Arrays.asList("enable_streaming_engine");
      options.setExperiments(expNames_1);
      options.setJobName("Test2");
      runWindowedTrading(options);
    }
  }