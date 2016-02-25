<?php

namespace OLAP;

use \Doctrine\DBAL\Connection;
use OLAP\DB;

class CubeFactory {

    /**
     * @var DB\Cube[]
     */
    static private $cubes = [];

    /**
     * @param Connection $db
     * @param Cube $cube
     * @return DB\Cube
     */
    static public function getCube(Connection $db, Cube $cube) {

        if (empty(self::$cubes[$cube->getName()])) {
            self::$cubes[$cube->getName()] = new DB\Cube($db, $cube);
        }
        return self::$cubes[$cube->getName()];
    }
}