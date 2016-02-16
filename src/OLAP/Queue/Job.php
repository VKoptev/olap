<?php

namespace OLAP\Queue;


use JobQueue\JobBase;
use JobQueue\Options;
use OLAP\Server;

class Job extends JobBase {

    protected function execute() {

        $server = Options::getInstance()->get('olapServer');

        if ($server instanceof Server && !empty($this->data['data'])) {
            $server->setData($this->data['data']);
        }
        $this->finishJob();
    }
}