<?php

namespace OLAP\DB;

use Doctrine\DBAL\Connection;


/**
 * Class Cube
 * @package OLAP\DB
 * @method \OLAP\Cube object()
 */
class Cube extends Base {

    /**
     * @var Dimension[]
     */
    private $dimensions = [];

    /**
     * @var Type
     */
    private $dataType;

    /**
     * @param Connection $db
     * @param \OLAP\Cube $object
     * @param Base $sender
     */
    public function __construct(Connection $db, \OLAP\Cube $object, Base $sender = null) {

        parent::__construct($db, $object, $sender);

        $this->dimensions = [];
        foreach ($this->object()->getDimensions() as $dimension) {
            $this->dimensions[$dimension->getName()] = new Dimension($this->db(), $dimension, $this);
        }
        $this->dataType = new Type($this->db(), $this->object()->getDataType());
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

        return $this->dataType;
    }

    /**
     * Check structure of cube
     */
    public function checkStructure() {

        $this->getDataType()->checkStructure();

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
        foreach ($this->getDimensions() as $dimension) {

            $fields[]       = "{$dimension->getTableName()}_id integer";
            $key[]          = "{$dimension->getTableName()}_id";
            $constraints[]  = "CONSTRAINT {$this->getTableName()}_{$dimension->getTableName()}_id_fkey FOREIGN KEY ({$dimension->getTableName()}_id) " .
                              "REFERENCES {$dimension->getTableName()} (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION";
        }
        $key = implode(",",   $key);
        $constraints[] = "CONSTRAINT {$this->getTableName()}_unique UNIQUE ($key)";

        $fields         = implode(",", $fields);
        $constraints    = implode(",", $constraints);

        $this->db()->exec("CREATE TABLE public.{$this->getTableName()} ($fields, $constraints) WITH(OIDS=FALSE);");
    }
}