<?php

namespace OLAP;

use \Doctrine\DBAL\Connection;

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
        $this->cube = $cube;
    }

    /**
     * Check server DB structure; create non-existed dimensions and facts
     * @throws \Doctrine\DBAL\DBALException
     */
    public function checkStructure() {

        // check dimensions
        $sql = [];
        $dimensions = $this->cube->getDimensions();
        foreach ($dimensions as $dimensionName => $dimension) {
            $sql[] = "to_regclass('public.$dimensionName')" . ' as ' . $dimensionName;
        }
        $result = $this->db->query("SELECT " . implode(', ', $sql))->fetch();
        foreach ($result as $dimensionName => $exists) {
            if ($exists) {
                $this->checkDimension($dimensions[$dimensionName]);
            } else {
                $this->createDimension($dimensions[$dimensionName]);
            }
        }

        // check facts
        $facts = strtolower($this->cube->getName());
        $result = $this->db->fetchColumn("SELECT to_regclass('public.$facts')");
        if (!$result) {
            $this->createFacts();
        }
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

    private function createDimension(Dimension $dimension) {

        $tableName = $dimension->getName();
        $type = $dimension->getType();
        $this->checkType($type);
        $data = '';
        if ($dimension->isDenormalized()) {
            $this->checkType($this->cube->getDataType());
            $data = $this->cube->dataField() . ' ' . $this->cube->getDataType()->getType() . ',';
        }
        $sql = <<<SQL
CREATE TABLE public.$tableName (
  id serial NOT NULL,
  {$this->cube->valueField()} {$type->getType()},
  $data
  CONSTRAINT {$tableName}_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
CREATE INDEX {$tableName}_{$this->cube->valueField()}_idx
  ON public.{$tableName}
  USING {$dimension->getIndex()}
  ({$this->cube->valueField()});
SQL;

        $this->db->exec($sql);

    }

    private function checkDimension(Dimension $dimension) {

        $dimensionName = strtolower($dimension->getName());
        $columns = [];
        foreach ($this->db->fetchAll(
            "SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_name = :tblname",
            [':tblname' => $dimensionName]) as $row) {

            $columns[$row['column_name']] = strtolower($row['data_type']) === 'user-defined' ? $row['udt_name'] : $row['data_type'];
        }
        if ($dimension->getType()->getType() != $columns[$this->cube->valueField()]) {
            $this->checkType($dimension->getType());
            // simple switch type; but it may be unavailable
            $using = $dimension->getType()->getUsing();
            $using = $using ? "USING $using" : '';
            $this->db->exec("ALTER TABLE public.$dimensionName ALTER COLUMN {$this->cube->valueField()} TYPE " . $dimension->getType()->getType() . " $using");
        }
         // todo: changing index of value and changing data type
    }

    private function checkType(Type $type) {

        $exists = $this->db->fetchColumn("SELECT EXISTS (select 1 from pg_catalog.pg_type where typname = :typname)", [':typname' => $type->getType()]);
        if (!$exists && $creation = $type->getCreation()) {
            $this->db->exec($creation);
        }
    }

    private function createFacts() {

        $this->checkType($this->cube->getDataType());

        $tableName = strtolower($this->cube->getName());
        $names = array_keys($this->cube->getDimensions());
        $fields = implode("_id integer,\n", $names) . '_id integer';
        $pk = implode('_id, ', $names) . '_id';
        $constraints = [];
        foreach ($names as $name) {
            $constraints[] = "CONSTRAINT {$tableName}_{$name}_id_fkey FOREIGN KEY ({$name}_id) " .
                             "REFERENCES {$name} (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION";
        }
        $constraints = $constraints ? ',' . implode(',', $constraints) : '';

        $data = $this->cube->dataField() . ' ' . $this->cube->getDataType()->getType();
        $sql = <<<SQL
CREATE TABLE public.$tableName (
  id serial NOT NULL,
  $fields,
  $data,
  CONSTRAINT {$tableName}_pkey PRIMARY KEY (id),
  CONSTRAINT {$tableName}_unique UNIQUE ($pk)
  $constraints

)
WITH (
  OIDS=FALSE
);
SQL;
        $this->db->exec($sql);
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