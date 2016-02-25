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

        $keys = $this->getKeys();
        $fields = array_diff($keys, $this->getParent()->getKeys());
        list($field,) = each($fields);
        $dimension = $this->getDimension($field);
        $list = $dimension->getIds($data);
        foreach ($list as $value) {
            $data[$dimension->getTableName()] = $value["{$dimension->getTableName()}_id"];
            $this->pushData($data);
        }

        date_default_timezone_set($currentTZ);
    }

    public function pushData(array $data) {

        $where  = [];
        $params = [];
        $fields = $this->getKeys();
        foreach ($fields as $key => $field) {
            $params[":$key"] = $data[$key];
            $where[] = "$field " . ($data[$key] === null ? 'IS NULL' : "= :$key");
        }
        $where = $where ? 'WHERE (' . implode(') AND (', $where) . ')' : '';

        $this->updateData($this->sender()->getPusher($data), $fields, $where, $params, $data);
    }

    protected function dimensionClass() {

        return Dimension::class;
    }

    protected function addFactId() {

        return false;
    }

    protected function getKeys() {

        $keys = parent::getKeys();
        $parentKeys = $this->getParent()->getKeys();
        if (($dimension = $this->object()->getSpecialDimension()) && isset($parentKeys[$dimension])) {
            unset($parentKeys[$dimension]);
            $keys = array_merge($keys, $parentKeys);
        }
        return $keys;
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