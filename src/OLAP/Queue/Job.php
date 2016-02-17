<?php

namespace OLAP\Queue;


use JobQueue\JobBase;
use OLAP\Server;

abstract class Job extends JobBase {

    protected function execute() {

        $server = $this->getServer();

        if ($server instanceof Server && !empty($this->data['data'])) {
            $server->setData($this->data['data']);
        }
        $this->finishJob();
    }

    /**
     * @return Server
     */
    abstract protected function getServer();
}