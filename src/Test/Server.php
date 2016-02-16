<?php

namespace Test;


use JobQueue\JobFabric;
use OLAP\Event;

class Server {

    static public function fillData(\OLAP\Server $server) {

        $server->truncate();
        foreach (self::getDataChunk() as $doc) {
            JobFabric::getInstance()->createJob(
                (new JobBuilder())->setData($doc)
            );
        }
    }

    static public function testCheckStructure(\OLAP\Server $server) {

        $server->checkStructure();
    }

    static public function testSetData(\OLAP\Server $server) {

        foreach (self::getDataChunk() as $doc) {
            $server->setData($doc);
        }
    }

    static public function testMultiThreadAggregation(\OLAP\Server $server, $cmd, $dates) {

        if ($dates) {
            echo "start " . $dates . "\n";
            $dates = json_decode($dates, true);
            if ($dates) {

                Event\Ruler::getInstance()->trigger(Event\Type::EVENT_SET_ALL_DATA, null, ['date' => $dates]);
            }
        } else {
            $n = 10;
            $start = strtotime("2014-12-31 21:00:00");
            $end   = strtotime(date("2015-02-13 23:00:00", strtotime('+1day')));
            $step  = floor(($end - $start) / $n);
            $workers = [];
            for ($i = $start; $i < $end; $i += $step) {

                $worker = (object)[
                    'cmd' => str_replace('%thread%', '"' . addslashes(json_encode(['from' => date('Y-m-d H:i:s', $i), 'to' => date('Y-m-d H:i:s', $i + $step)])) . '"', $cmd),
                    'descriptorspec' => [
                        0 => ["pipe", "r"],
                        1 => ["pipe", "w"],
                        2 => ["file","/dev/null", "w"]
                    ],
                    'pipe' => [],
                    'resource' => null,
                ];
                $worker->resource = proc_open($worker->cmd, $worker->descriptorspec, $worker->pipe);

                if (is_resource($worker->resource)) {
                    $workers[] = $worker;
                }
            }
            while($workers) {
                foreach ($workers as $i => $worker) {
                    stream_set_blocking($worker->pipe[1], 0);
                    echo stream_get_contents($worker->pipe[1]);
                    flush();
                    $status = proc_get_status($worker->resource);
                    if (!$status['running']) {
                        fclose($worker->pipe[0]);
                        fclose($worker->pipe[1]);
                        proc_close($worker->resource);
                        unset($workers[$i]);
                    }
                }
                sleep(5);
            }
        }
    }

    static private function getDataChunk() {

        $mongo = new \MongoClient();
        $db = $mongo->selectDB('admin');
        $tracks = $db->tracks;

        $pipeline = [
            ['$match' => []],
            ['$group' => [
                '_id' => [
                    'hour' => ['$dateToString' => ['format' => '%Y-%m-%d %H:00:00', 'date' => '$createdAt']],
                    'pid' => '$pid',
                    'supplier' => '$supplier',
                    'offer' => '$programId',
                    'country' => '$country',
                    'city_id' => '$city_id',
                    'os'   => '$track_info.os.family',
                    'browser'   => '$track_info.browser.family',
                    'device'   => '$track_info.device.family',
                    'device_model' => '$track_info.device.model',
                    'sub1'   => '$sub',
                    'sub2'   => '$sub2',
                    'sub3'   => '$sub3',
                    'sub4'   => '$sub4',
                    'sub5'   => '$sub5',
                ],
                'raw' => ['$sum' => '$count'],
                'uniq' => ['$sum' => 1],
            ]],
            ['$project' => [
                'hour' => '$_id.hour',
                'pid' => '$_id.pid',
                'raw' => '$raw',
                'uniq' => '$uniq',
                'supplier' => '$_id.supplier',
                'offer' => '$_id.offer',
                'country' => '$_id.country',
                'city_id' => '$_id.city_id',
                'os' => '$_id.os',
                'browser' => '$_id.browser',
                'device' => '$_id.device',
                'device_model' => '$_id.device_model',
                'sub1' => '$_id.sub1',
                'sub2' => '$_id.sub2',
                'sub3' => '$_id.sub3',
                'sub4' => '$_id.sub4',
                'sub5' => '$_id.sub5',
            ]]
        ];
        for ($i = -300; $i < -299; $i++){
            $start  = strtotime(date('Y-m-d 00:00:00', strtotime($i . 'days')));
            $end    = strtotime(date('Y-m-d 00:00:00', strtotime(($i + 1) . 'days')));
            echo date('Y-m-d', $start) . "...\n";
            $pipeline[0]['$match'] = [
                'createdAt' => ['$gte' => new \MongoDate($start), '$lt' => new \MongoDate($end)]
            ];
            $aggr = $tracks->aggregate($pipeline);

            if (!empty($aggr['result'])) {
                foreach ($aggr['result'] as $doc) {
                    unset($doc['_id']);
                    foreach ($doc as &$value) {
                        $value = $value instanceof \MongoId ? (string) $value : $value;
                    }
                    yield $doc;
                }
            }
        }
    }
}