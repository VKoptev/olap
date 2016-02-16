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

$db = \Doctrine\DBAL\DriverManager::getConnection([
    'host' => 'localhost',
    'port' => 5432,
    'dbname' => 'olap',
    'user' => 'user',
    'password' => 'user',
    'driver' => 'pdo_pgsql'
]);
$logger = new \Test\EchoSQLLogger();
if (1) $db->getConfiguration()->setSQLLogger($logger);

$cube = \Test\Cube::get();
$server = new OLAP\Server($db, $cube);


$mongo = new \MongoClient();

\JobQueue\Options::getInstance()->setOptions([
    'mongo' => $mongo->selectDB('olap_queue'),
    'worker_cmd' => "php " . __FILE__ . " worker",
    'worker_max_count' => 30,
    'job_types' => [
        \Test\JobBuilder::TYPE => \OLAP\Queue\Job::class
    ],
    'log' => function($msg) {
        $date = date('Y-m-d H:i:s');
        file_put_contents('/tmp/job.log', "[$date]: $msg\n", FILE_APPEND);
    },
    'olapServer' => $server
]);

if (!empty($argv[1])) {
    switch($argv[1]) {
        case 'worker':
            (new \JobQueue\Worker($argv[2]))->run();
            break;
        case 'dispatcher':
            (new \JobQueue\Dispatcher())->run();
            break;
        case 'set-data':
            \Test\Server::fillData($server);
            break;
    }
}

$logger->getSummary();
