<?php

use Elasticsearch\ClientBuilder;

class EsUtils {

    static private $verbose;

    private static function message($msg) {
        if(self::$verbose) {
            fwrite(STDERR, "$msg\n");
        }
    }

    /**
     * 把数据从一个 index copy 到另一个 index
     */
    public static function copy($options) {

        gc_enable();

        $getOption = function($key,$default=NULL) use (&$options) {
            if(array_key_exists($key,$options)) {
                return $options[$key];
            } else {
                return $default;
            }
        };

        $fromIndex = $getOption('fromIndex');
        $fromType  = $getOption('fromType');
        $fromHosts = $getOption('fromHosts');
        $query     = $getOption('query',['match_all'=>[]]);
        $scroll    = $getOption('scroll','60s');
        $batchSize = $getOption('batchSize',1000);
        $toIndex   = $getOption('toIndex');
        $toType    = $getOption('toType',$fromType);
        $toHosts   = $getOption('toHosts',[]);
        $transform = $getOption('transform');
        $verbose   = $getOption('verbose',true);

        self::$verbose = $verbose;

        $cli1 = ClientBuilder::create()->setHosts($fromHosts)->build();
        if(count(array_diff($fromHosts,$toHosts))==0 && count(array_diff($toHosts,$fromHosts))==0) {
            $cli2 = $cli1;
        } else if(empty($toHosts)) {
            $cli2 = $cli1;
        } else {
            $cli2 = ClientBuilder::create()->setHosts($toHosts)->build();
        }

        $params = [
            "scroll" => $scroll,        // how long between scroll requests. should be small!
            "body" => [
                "query" => $query,
            ],
            "size" => $batchSize,       // how many results *per shard* you want back
            "index" => $fromIndex,
            "sort" => [
                "_doc",
            ],
        ];

        $count = 0;
        $time = 0;
        $docs = $cli1->search($params);
        while(count($docs['hits']['hits'])!=0) {

            if($time+60<=time()) {
                // force gc
                gc_collect_cycles();
                $time=time();
                self::message("time: [$time] count:[$count]");
            }   

            $scrollId = $docs['_scroll_id'];

            $params = ['body' => []];

            foreach($docs['hits']['hits'] as & $hit) {

                ++$count;

                $e = [
                    'create' => [
                        '_index' => $toIndex,
                        '_type' => $toType,
                        '_id' => $hit['_id'],
                    ],
                ];
                if(array_key_exists('_routing',$hit)) {
                    $e['create']['_routing'] = $hit['_routing'];
                }

                // var_dump($hit);
                $params['body'][] = $e;

                $params['body'][] = $hit['_source'];

            }

            if($transform) {
                for($i=0; $i<count($params['body']); $i+=2) {
                    $transform($params['body'][$i]['index'],$params['body'][$i+1]);
                }
            }

            $responses = $cli2->bulk($params);

            $params = [
                "scroll" => $scroll,        // how long between scroll requests. should be small!
                "scroll_id" => $scrollId,
            ];
            $docs = $cli1->scroll($params);
        }

        return $count;
    }

    /**
     * 把数据从一个 index copy 到另一个 index
     */
    public static function delete($options) {

        gc_enable();

        $getOption = function($key,$default=NULL) use (&$options) {
            if(array_key_exists($key,$options)) {
                return $options[$key];
            } else {
                return $default;
            }
        };

        $index     = $getOption('index');
        $type      = $getOption('type');
        $hosts     = $getOption('hosts');
        $query     = $getOption('query',['match_all' =>[]]);
        $batchSize = $getOption('batchSize',1000);
        $verbose   = $getOption('verbose',true);

        self::$verbose = $verbose;

        $client = ClientBuilder::create()->setHosts($hosts)->build();

        $count = 0;
        $total = 0;
        $time = 0;
        while(true) {

            if($time<time()) {
                // force gc
                gc_collect_cycles();
                $time=time();
                self::message("time: [$time] count:[$count]");
                $count = 0;
            }   

            $params = [];
            $params['index']         = $index;
            $params['type']          = $type;
            $params['size']          = $batchSize;
            $params['fields']        = [];
            $params['body']          = [];
            $params['body']['query'] = $query;
            $ret = $client->search($params);
            $hits = $ret['hits']['hits'];
            if(count($hits)==0) {
                break;
            }
            $count += count($hits);
            $total += count($hits);

            $params = [];
            $params['body'] = [];
            foreach($hits as $hit) {
                $e = [
                    'delete' => [
                        '_index' => $index,  
                        '_type' => $type,  
                        '_id' => $hit['_id'],
                    ]
                ];
                if(array_key_exists('_routing',$hit)) {
                    $e['delete']['_routing'] = $hit['_routing'];
                }
                $params['body'][] =  $e;
            }

            $client->bulk($params);
            $client->indices()->refresh(['index'=>$index]);
        }

        self::message("time: [$time] total count:[$total]");

        return $count;
    }
}


// require 'vendor/autoload.php';
// $options['fromIndex'] = "my_index"; 
// $options['fromType']  = "my_type"; 
// $options['fromHosts'] = ["http://localhost:9201"]; 
// /* $options['query']     = []; */ 
// $options['scroll']    = '10s';
// $options['batchSize'] = 1000;
// $options['toIndex']   = "my_index2"; 
// $options['toType']    = "my_type2"; 
// $options['toHosts']   = []; 
// $options['verbose']   = true; 
// $options['transform'] = function(&$index,&$source) {
//         // $index['_id'] = 'abc'.$index['_id'];
//     };
// 
// var_dump(EsUtils::copy($options));

// require 'vendor/autoload.php';
// $options['index']     = "my_index";
// $options['type']      = "my_type";
// $options['hosts']     = ["http://localhost:9200"];
// $options['batchSize'] = 1;
// 
// var_dump(EsUtils::delete($options));
// 
