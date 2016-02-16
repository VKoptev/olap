<?php

namespace Test;



use Doctrine\DBAL\Logging\SQLLogger;

class EchoSQLLogger implements SQLLogger
{
    private $time = 0;
    private $counter = 0;
    private $summaryTime = 0;
    /**
     * {@inheritdoc}
     */
    public function startQuery($sql, array $params = null, array $types = null)
    {
        $this->time = microtime(true);
//        echo "<pre>$sql</pre>" . PHP_EOL;

//        if ($params) {
//            var_dump($params);
//        }

//        if ($types) {
//            var_dump($types);
//        }
    }

    /**
     * {@inheritdoc}
     */
    public function stopQuery()
    {

        $diff = microtime(true) - $this->time;
        $this->counter++;
        $this->summaryTime += $diff;
//        echo "<pre>Time execution: $diff</pre>" . PHP_EOL;
    }

    public function getSummary() {

        echo "<pre>Queries: {$this->counter}\nTime execution: {$this->summaryTime}</pre>" . PHP_EOL;
    }
}