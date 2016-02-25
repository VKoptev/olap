<?php
namespace OLAP;

use \Doctrine\DBAL\Connection;
use OLAP\DB;

class Client {

    /**
     * @var Connection
     */
    private $db;

    /**
     * @var Cube
     */
    private $cube;


    public function __construct(Connection $connection, Cube $cube) {

        $this->db = $connection;
        $this->cube = CubeFactory::getCube($connection, $cube);
    }

    public function slice($filter) {

        $this->cube->slice($filter);
        return $this;
    }

    public function drill($drill) {

        $this->cube->drill($drill);
        return $this;
    }

    public function get() {

        return $this->cube->get();
    }
}