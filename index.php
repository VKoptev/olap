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

if (!empty($argv[1])) {
    switch($argv[1]) {
        case 'set-data':
            \Test\Server::testCheckStructure($server);
            \Test\Server::testSetData($server);
            break;
        case 'test-multi-threading':
            \Test\Server::testMultiThreadAggregation($server, "php " . __FILE__ . " test-multi-threading %thread%", empty($argv[2]) ? null : $argv[2]);
            break;
    }
}

$logger->getSummary();
