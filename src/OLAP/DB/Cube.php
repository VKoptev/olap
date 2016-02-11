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
     * @var Fact[]
     */
    private $facts = [];

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

        $this->facts = [];
        foreach ($this->object()->getFacts() as $fact) {
            $this->facts[$fact->getName()] = new Fact($this->db(), $fact, $this);
        }
        $this->dataType = new Type($this->db(), $this->object()->getDataType());
    }

    /**
     * @return Fact[]
     */
    public function getFacts() {

        return $this->facts;
    }

    /**
     * @param $name
     * @return Fact
     */
    public function getFact($name) {

        return empty($this->facts[$name]) ? false : $this->facts[$name];
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

        foreach ($this->getFacts() as $fact) {
            $fact->checkStructure();
        }
    }

    /**
     * @return string
     */
    public function dataField() {

        return $this->object()->dataField();
    }

    /**
     * @return string
     */
    public function valueField() {

        return $this->object()->valueField();
    }
}