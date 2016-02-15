<?php

namespace OLAP\DB\SpecialFact;


use OLAP\DB\Fact;
use OLAP\DB\SpecialFact\Timezone\Dimension;
use OLAP\DB\UserQuery;

/**
 * Class Timezone
 * @package OLAP\DB\SpecialFact
 * @method \OLAP\SpecialFact\Timezone object()
 */
class Timezone extends Fact {

    private $aggregatedData = [];

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

        $where  = [];
        $params = [];
        $fields = $this->getKeys();
        foreach ($this->getParent()->getKeys() as $key => $field) {
            if (isset($fields[$key])) {
                $params[":$key"] = $data[$key];
                $where[] = "$fields[$key] " . ($data[$key] === null ? 'IS NULL' : "= :$key");
            }
        }
        $parentWhere = $where;
        $parentParams = $params;
        $values = [];
        $param = null;
        foreach ($fields as $key => $field) {
            if (!isset($params[":$key"]) && ($dimension = $this->getDimension($key))) {
                $values = $dimension->getIds($data);
                $param = $key;
                $params[":$key"] = null;
                $where[] = "$fields[$key] = :$key";
                break;
            }
        }
        $where = $where ? 'WHERE (' . implode(') AND (', $where) . ')' : '';

        $this->aggregatedData = [];
        foreach ($values as $value) {
            $params[":$param"] = $value["{$param}_id"];

            $setter = $this->sender()->getSetter($this->getAggregatedData($value, $parentWhere, $parentParams));

            $this->updateData($setter, $fields, $where, $params, $data);
        }
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

    protected function getAggregatedData($value, $parentWhere, $parentParams) {

        $timezone = new \DateTimeZone($value['timezone']);
        $offset = $timezone->getOffset(new \DateTime());
        $key = "{$value['value']} $offset";
        if (!isset($this->aggregatedData[$key])) {
            $date = new \DateTime($value['value'], $timezone);
            $date->setTimezone(new \DateTimeZone('UTC'));

            $special = $this->getSpecialDimension(false);
            $parentWhere[] = "{$special->getTableName()}.{$this->valueField()} >= :{$special->getTableName()}_start";
            $parentWhere[] = "{$special->getTableName()}.{$this->valueField()} < :{$special->getTableName()}_end";
            $parentParams[":{$special->getTableName()}_start"] = $date->format('Y-m-d H:i:s');
            $parentParams[":{$special->getTableName()}_end"] = $date->modify("+1day")->format('Y-m-d H:i:s');

            $where = $parentWhere ? 'WHERE (' . implode(') AND (', $parentWhere) . ')' : '';

            $aggregator = $this->sender()->getAggregateLinear($this->getParent()->getTableName());
            $sql = "SELECT {$aggregator->getQuery()} FROM {$this->getParent()->getTableName()} " .
                   "INNER JOIN {$special->getTableName()} ON {$special->getTableName()}.id = {$this->getParent()->getTableName()}.{$special->getTableName()}_id " .
                   "$where"
            ;
            $this->aggregatedData[$key] = $this->db()->fetchAssoc($sql, array_merge($parentParams, $aggregator->getParams()));
        }
        return $this->aggregatedData[$key];
    }
}