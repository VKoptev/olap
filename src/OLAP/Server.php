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
     * @var Cube
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

    /**
     * Set cube data
     * @param array $data
     * @param array $map
     */
    public function setData(array $data, array $map = []) {

        $this->db->beginTransaction();

        // collect dimension ids
        $ids = [];
        foreach ($this->cube->getDimensions() as $dimensionName => $dimension) {
            $key = $dimensionName . '_id';
            $value = $dimension->getData($data, $map);
            $ids[$key] = $this->getDimensionId($dimension, $value);
        }

        // collect prepared data
        $where  = [];
        $update = [];
        $insert = [];
        $params = [];
        foreach ($ids as $key => $value) {
            $params[':' . $key] = $value;
            $where[] = "$key " . ($value === null ? 'IS NULL' : "= :$key");
            $update[] = "$key = :$key";
            $insert[] = $key;
        }
        $where = $where ? 'WHERE (' . implode(') AND (', $where) . ')' : '';
        $tableName = strtolower($this->cube->getName());

        // check existence
        $id = $this->db->fetchColumn("SELECT id FROM $tableName $where", $params);
        if (!$id) {
            // insert
            $sql = "INSERT INTO $tableName (" . implode(', ', $insert) . ") VALUES(" . implode(',', array_keys($params)) . ") RETURNING id";
            $id = $this->db->fetchColumn($sql, $params);
        }
        list($setter, $params) = $this->cube->getSetter($data);
        $params[':id'] = $id;
        $this->db->fetchArray("UPDATE $tableName SET $setter WHERE id = :id", $params);


        foreach ($this->cube->getDimensions() as $dimensionName => $dimension) {
            if ($dimension->isDenormalized()) {

                $key = $dimensionName . '_id';
                list($aggregate, $params) = $this->cube->getAggregate();
                $params[':' . $key] = $ids[$key];
                $result = $this->db->fetchColumn("SELECT $aggregate FROM $tableName WHERE $key = :$key", $params);

                $tblName = strtolower($dimension->getName());
                $this->db->fetchColumn("UPDATE $tblName SET {$this->cube->dataField()} = :data WHERE id = :id", [':id' => $ids[$key], ':data' => $result]);
            }
        }

        $this->db->commit();
    }

    private function getDimensionId(Dimension $dimension, $value) {

        $tblName = strtolower($dimension->getName());
        if ($value === null) {
            $id = $this->db->fetchColumn("SELECT id from $tblName WHERE {$this->cube->valueField()} IS NULL ORDER BY id LIMIT 1");
        } else {
            $id = $this->db->fetchColumn("SELECT id from $tblName WHERE {$this->cube->valueField()} = :value ORDER BY id LIMIT 1",
                [':value' => $value]);
        }
        if (!$id) {
            $id = $this->db->fetchColumn("INSERT INTO $tblName ({$this->cube->valueField()}) VALUES(:value) RETURNING id", [':value' => $value]);
        }
        return $id;
    }

    private function pushDimensionData($id, Dimension $dimension, $data) {

        $tblName = strtolower($dimension->getName());
        list($pusher, $params) = $this->cube->getPusher($data);

        $params[':id'] = $id;
        $this->db->fetchColumn("UPDATE $tblName SET $pusher WHERE id = :id", $params);
    }
}