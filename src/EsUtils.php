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
        $verbose   = $getOption('verbose',false);

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

                // var_dump($hit);
                $params['body'][] = [
                    'index' => [
                        '_index' => $toIndex,
                        '_type' => $toType,
                        '_id' => $hit['_id'],
                    ]
                ];

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
}


// require 'vendor/autoload.php';
// $options['fromIndex'] = "bigbanglib_wish"; 
// $options['fromType']  = "data"; 
// $options['fromHosts'] = ["http://10.104.67.229:9201"]; 
// /* $options['query']     = []; */ 
// $options['scroll']    = '10s';
// $options['batchSize'] = 2000;
// $options['toIndex']   = "bigbanglib_wish_v2"; 
// $options['toType']    = "data"; 
// $options['toHosts']   = []; 
// $options['verbose']   = true; 
// $options['transform'] = function(&$index,&$source) {
//         $index['_id'] = 'abc'.$index['_id'];
//     };
// 
// var_dump(EsUtils::copy($options));

