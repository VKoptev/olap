<?php

namespace OLAP;

use \Doctrine\DBAL\Connection;
use OLAP\DB;

class Server {

    /**
     * @var Connection
     */
    private $db;

    /**
     * @var DB\Cube
     */
    private $cube;


    public function __construct(Connection $connection, Cube $cube) {

        $this->db = $connection;
        $this->cube = new DB\Cube($this->db, $cube);
    }

    /**
     * Check server DB structure; create non-existed dimensions and facts
     * @throws \Doctrine\DBAL\DBALException
     */
    public function checkStructure() {

        $this->cube->checkStructure();
    }

    public function setData(array $data) {

        $this->cube->setData($data);
    }

    public function truncate() {

        $this->checkStructure();
        $this->cube->truncate();
    }
}