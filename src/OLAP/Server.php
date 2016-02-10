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

    private function createDimension(Dimension $dimension) {

        $tableName = $dimension->getName();
        $type = $dimension->getType();
        $this->checkType($type);
        $data = '';
        if ($dimension->isDenormalized()) {
            $this->checkType($this->cube->getDataType());
            $data = 'data ' . $this->cube->getDataType()->getType() . ',';
        }
        $sql = <<<SQL
CREATE TABLE public.$tableName (
  id serial NOT NULL,
  value {$type->getType()},
  $data
  CONSTRAINT {$tableName}_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
CREATE INDEX {$tableName}_value_idx
  ON public.{$tableName}
  USING {$dimension->getIndex()}
  (value);
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
        if ($dimension->getType()->getType() != $columns['value']) {
            $this->checkType($dimension->getType());
            // simple switch type; but it may be unavailable
            $using = $dimension->getType()->getUsing();
            $using = $using ? "USING $using" : '';
            $this->db->exec("ALTER TABLE public.$dimensionName ALTER COLUMN value TYPE " . $dimension->getType()->getType() . " $using");
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

        $data = 'data ' . $this->cube->getDataType()->getType();
        $sql = <<<SQL
CREATE TABLE public.$tableName (
  $fields,
  $data,
  CONSTRAINT {$tableName}_pkey PRIMARY KEY ($pk)
  $constraints

)
WITH (
  OIDS=FALSE
);
SQL;
        $this->db->exec($sql);
    }
}