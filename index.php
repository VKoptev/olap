<?php

error_reporting(E_ALL);
define('ROOT', dirname(__FILE__));
ini_set('display_errors', true);

require_once(__DIR__."/vendor/autoload.php");

spl_autoload_register(function($className) {

    $className = str_replace('\\', '/', $className);
    foreach ([ROOT . '/lib/' . $className . '.php',ROOT . '/src/' . $className . '.php'] as $fileName ) {
        if (file_exists($fileName)) {
            require_once $fileName;
        }
    }
});

class EchoSQLLogger implements \Doctrine\DBAL\Logging\SQLLogger
{
    private $time = 0;
    /**
     * {@inheritdoc}
     */
    public function startQuery($sql, array $params = null, array $types = null)
    {
        $this->time = microtime(true);
        echo "<pre>$sql</pre>" . PHP_EOL;

        if ($params) {
            var_dump($params);
        }

        if ($types) {
            var_dump($types);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function stopQuery()
    {

        echo "<pre>Time execution: " . (microtime(true) - $this->time) . "</pre>" . PHP_EOL;
    }
}
$db = \Doctrine\DBAL\DriverManager::getConnection([
    'host' => 'localhost',
    'port' => 5432,
    'dbname' => 'olap',
    'user' => 'user',
    'password' => 'user',
    'driver' => 'pdo_pgsql'
]);
if (0) $db->getConfiguration()->setSQLLogger(new EchoSQLLogger());
$cube = new \OLAP\Cube(
    'tracks',
    [ // facts
        'hour' => [
            'name' => 'hour',
            'dimensions' => [
                [
                    'name' => 'hour',
                    'type' => 'timestamp without time zone',
                    'denormalized' => true,
                    'index' => 'btree',
                ],
                [
                    'name' => 'pid',
                    'type' => 'integer',
                ],
                [
                    'name' => 'supplier',
                    'type' => 'integer',
                ],
                [
                    'name' => 'offer',
                    'type' => 'integer',
                    'parent' => 'supplier',
                ],
                [
                    'name' => 'country',
                    'type' => 'character varying(5)',
                ],
                [
                    'name' => 'city_id',
                    'type' => 'integer',
                    'parent' => 'country',
                ],
                [
                    'name' => 'os',
                    'type' => 'character varying(255)',
                ],
                [
                    'name' => 'browser',
                    'type' => 'character varying(255)',
                ],
                [
                    'name' => 'device',
                    'type' => 'character varying(255)',
                ],
                [
                    'name' => 'device_model',
                    'type' => 'character varying(255)',
                    'parent' => 'device',
                ],
            ]
        ],
        'date' => [
            'name' => 'date',
            'special' => 'timezone',
            'parent' => 'hour',
            'dimension' => 'hour',
        ],
        'sub_hour' => [
            'name' => 'sub_hour',
            'parent' => 'hour',
            'dimensions' => [
                [
                    'name' => 'sub1',
                    'type' => 'character varying(255)',
                ],
                [
                    'name' => 'sub2',
                    'type' => 'character varying(255)',
                ],
                [
                    'name' => 'sub3',
                    'type' => 'character varying(255)',
                ],
                [
                    'name' => 'sub4',
                    'type' => 'character varying(255)',
                ],
                [
                    'name' => 'sub5',
                    'type' => 'character varying(255)',
                ],
            ],
        ],
    ],
    [
        'name' => 'info',
        'create' => 'CREATE TYPE info AS("raw" integer,uniq integer)',
        'aggregate' => 'Row(SUM((%DATA_FIELD%).raw), SUM((%DATA_FIELD%).uniq))::%DATA_TYPE%',
        'set_data' => '%DATA_FIELD%.raw = %raw%, %DATA_FIELD%.uniq = %uniq%',
        'push_data' => <<<SQL
%DATA_FIELD%.raw = COALESCE((%DATA_FIELD%).raw, 0) + %raw%,
%DATA_FIELD%.uniq = COALESCE((%DATA_FIELD%).uniq, 0) + %uniq%
SQL

    ]);

$server = new OLAP\Server($db, $cube);
$server->checkStructure();

//$mongo = new MongoClient();
//$db = $mongo->selectDB('admin');
//$tracks = $db->tracks;
//
$pipeline = [
    ['$match' => []],
    ['$group' => [
        '_id' => [
            'createdAt' => ['$dateToString' => ['format' => '%Y-%m-%d %H:00:00', 'date' => '$createdAt']],
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
        'createdAt' => '$_id.createdAt',
        'pid' => '$_id.pid',
        'raw' => '$raw',
        'uniq' => '$uniq',
        'country' => '$country',
        'city_id' => '$city_id',
        'os' => '$os',
        'browser' => '$browser',
        'device' => '$device',
        'device_model' => '$device_model',
        'sub1' => '$sub1',
        'sub2' => '$sub2',
        'sub3' => '$sub3',
        'sub4' => '$sub4',
        'sub5' => '$sub5',
    ]]
];
//for ($i = -450; $i < 0; $i++){
//    $start  = strtotime(date('Y-m-d 00:00:00', strtotime($i . 'days')));
//    $end    = strtotime(date('Y-m-d 00:00:00', strtotime(($i + 1) . 'days')));
//    if (abs($i) % 5 === 0) {
//        echo date('Y-m-d', $start) . "...\n";
//    }
//    $pipeline[0]['$match'] = [
//        'createdAt' => ['$gte' => new MongoDate($start), '$lt' => new MongoDate($end)]
//    ];
//    $aggr = $tracks->aggregate($pipeline);
//
//    if (!empty($aggr['result'])) {
//        foreach ($aggr['result'] as $doc) {
//            unset($doc['_id']);
//            $server->setData($doc);
//        }
//    }
//}
