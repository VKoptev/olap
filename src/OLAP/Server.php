<?php

namespace OLAP;

use \Doctrine\DBAL\Connection;

class Server {

    /**
     * @var Connection
     */
    private $db;
    /**
     * @var Dimension[]
     */
    private $dimensions = [];

    /**
     * @var Type
     */
    private $dataType;


    public function __construct(Connection $connection, $dimensions = [], $dataType = []) {

        $this->db = $connection;
        foreach ($dimensions as $dimension) {
            if (is_array($dimension) && !empty($dimension['name']) && !empty($dimension['type'])) {
                $dimension = new Dimension(
                    $dimension['name'],
                    $dimension['type'],
                    array_diff_key( $dimension, array_flip(['name', 'type']) )
                );
            }
            if ($dimension instanceof Dimension) {
                $this->dimensions[strtolower($dimension->getName())] = $dimension;
            }
        }
        $this->dataType = $dataType instanceof Type ? $dataType : new Type($dataType);
    }

    public function checkStructure() {

        // check dimensions
        $sql = [];
        foreach ($this->dimensions as $dimensionName => $dimension) {
            $sql[] = "to_regclass('public.$dimensionName')" . ' as ' . $dimensionName;
        }
        $result = $this->db->query("SELECT " . implode(', ', $sql))->fetch();
        foreach ($result as $dimensionName => $exists) {
            if ($exists) {
                $this->checkDimension($this->dimensions[$dimensionName]);
            } else {
                $this->createDimension($this->dimensions[$dimensionName]);
            }
        }
    }

    private function createDimension(Dimension $dimension) {

        $tableName = $dimension->getName();
        $type = $dimension->getType();
        $this->checkType($type);
        $data = '';
        if ($dimension->isDenormalized()) {
            $this->checkType($this->dataType);
            $data = 'data ' . $this->dataType->getType() . ',';
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
  USING hash
  (value);
SQL;

        $this->db->exec($sql);

    }

    private function checkDimension(Dimension $dimension) {


    }

    private function checkType(Type $type) {

        $exists = $this->db->fetchColumn("SELECT EXISTS (select 1 from pg_catalog.pg_type where typname = :typname)", [':typname' => $type->getType()]);
        if (!$exists && $creation = $type->getCreation()) {
            $this->db->exec($creation);
        }
    }
}