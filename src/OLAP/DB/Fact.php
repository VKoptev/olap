<?php

namespace OLAP\DB;

use Doctrine\DBAL\Connection;


/**
 * Class Cube
 * @package OLAP\DB
 * @method \OLAP\Fact object()
 * @method Cube sender()
 */
class Fact extends Base {

    /**
     * @var Dimension[]
     */
    private $dimensions = [];

    /**
     * @param Connection $db
     * @param \OLAP\Fact $object
     * @param Base $sender
     */
    public function __construct(Connection $db, \OLAP\Fact $object, Base $sender = null) {

        parent::__construct($db, $object, $sender);

        $this->dimensions = [];
        foreach ($this->object()->getDimensions() as $dimension) {
            $this->dimensions[$dimension->getName()] = new Dimension($this->db(), $dimension, $this);
        }
    }

    /**
     * @return Dimension[]
     */
    public function getDimensions() {

        return $this->dimensions;
    }

    /**
     * @return Type
     */
    public function getDataType() {

        return $this->sender()->getDataType();
    }

    /**
     * Check structure of cube
     */
    public function checkStructure() {

        foreach ($this->getDimensions() as $dimension) {
            $dimension->checkStructure();
        }

        parent::checkStructure();
    }

    /**
     * @return string
     */
    public function dataField() {

        return 'data';
    }

    /**
     * @return string
     */
    public function valueField() {

        return 'value';
    }

    /**
     * @param $name
     * @return Dimension
     */
    public function getDimension($name) {

        return empty($this->dimensions[$name]) ? false : $this->dimensions[$name];
    }

    protected function createTable() {

        $fields         = [
            "id serial NOT NULL",
            "{$this->dataField()} {$this->getDataType()->getTableName()}",
        ];
        $key            = [];
        $constraints    = [
            "CONSTRAINT {$this->getTableName()}_pkey PRIMARY KEY (id)"
        ];
        $parents = [];
        foreach ($this->getDimensions() as $dimension) {

            $fields[$dimension->getTableName()]       = "{$dimension->getTableName()}_id integer";
            $key[$dimension->getTableName()]          = "{$dimension->getTableName()}_id";
            $constraints[$dimension->getTableName()]  = $this->getFK($dimension->getTableName());
            if ($parent = $dimension->object()->getParent()) {
                $parents[$parent] = true;
            }
        }
        $fields         = array_diff_key($fields, $parents);
        $key            = array_diff_key($key, $parents);
        $constraints    = array_diff_key($constraints, $parents);

        if (($parent = $this->object()->getParent()) && ($parent = $this->sender()->getFact($parent))) {
            $fields[$parent->getTableName()] = "{$parent->getTableName()}_id integer";
            $constraints[$parent->getTableName()]  = $this->getFK($parent->getTableName());
        }

        $key = implode(",",   $key);
        $constraints[] = "CONSTRAINT {$this->getTableName()}_unique UNIQUE ($key)";

        $fields         = implode(",", $fields);
        $constraints    = implode(",", $constraints);

        $this->db()->exec("CREATE TABLE public.{$this->getTableName()} ($fields, $constraints) WITH(OIDS=FALSE);");
    }
}