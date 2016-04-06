<?php

namespace OLAP\DB\SpecialFact;


use Doctrine\DBAL\Connection;
use OLAP\DB\Cube;
use OLAP\Event;
use OLAP\DB\Fact;
use OLAP\DB\SpecialFact\Timezone\Dimension;

/**
 * Class Timezone
 * @package OLAP\DB\SpecialFact
 * @method \OLAP\SpecialFact\Timezone object()
 */
class Timezone extends Fact {

    private $aggregatedData = [];

    public function __construct(Connection $db, \OLAP\SpecialFact\Timezone $object, Cube $sender = null) {

        parent::__construct($db, $object, $sender);
    }

    /**
     * @param $name
     * @return Dimension
     */
    public function getDimension($name) {

        return parent::getDimension($name) ?: $this->getParent()->getDimension($name);
    }

    /**
     * @return Dimension
     */
    public function getSpecialDimension() {

        return $this->getDimension($this->object()->getSpecialDimension());
    }

    public function setData(array $data) {

        $currentTZ = date_default_timezone_get();
        date_default_timezone_set("UTC");


        date_default_timezone_set($currentTZ);
    }

    protected function dimensionClass() {

        return Dimension::class;
    }

    protected function addFactId() {

        return false;
    }

    protected function checkSetter()
    {

//        var_dump(123);
//        die();
    }

    protected function fillParentKeys($dimensions, $filter, &$used, &$joins, &$where, &$params) {

        $parents = $this->getParent()->getDimensions();
        if (($dimension = $this->object()->getSpecialDimension()) && isset($parents[$dimension])) {
            unset($parents[$dimension]);
            $dimensions = array_merge($dimensions, $parents);
        }

        parent::fillParentKeys($dimensions, $filter, $used, $joins, $where, $params);
    }
}