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

        Event\Ruler::getInstance()->addListener(new Event\Listener(Event\Type::EVENT_SET_ALL_DATA, [$this, 'onSetAllData']));
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

    public function onSetAllData($args) {

        $currentTZ = date_default_timezone_get();
        date_default_timezone_set("UTC");

        $params = [];
        $where = [];
        if (!empty($args['date']['from'])) {
            $where[] = "{$this->valueField()} >= :from";
            $params[':from'] = date('Y-m-d H:i:s', strtotime($args['date']['from']));
        }
        if (!empty($args['date']['to'])) {
            $where[] = "{$this->valueField()} <= :to";
            $params[':to'] = date('Y-m-d H:i:s', strtotime($args['date']['to']));
        }

        $where = $where ? 'WHERE (' . implode(') AND (', $where) . ')' : '';

        
        $special = $this->getSpecialDimension();
        $range = $this->db()->fetchAssoc("SELECT MIN({$this->valueField()}) as min, MAX({$this->valueField()}) as max FROM {$special->getTableName()} $where", $params);
        if (!$range['min']) {
            return;
        }

        $allFields = $this->getKeys();
        $fields = array_diff($allFields, $this->getParent()->getKeys());
        $keys = array_intersect($allFields, $this->getParent()->getKeys());
        list($field,) = each($fields);
        $dimension = $this->getDimension($field);
        $list = $dimension->getInfoByRange($range['min'], $range['max']);

        $interval = $dimension->object()->getOption('minimal-offset', '+1day');
        $group = implode(',', array_values($keys));
        $aggregator = $this->sender()->getAggregateLinear($this->getParent()->getTableName());
        $params = $aggregator->getParams();
        $cache = [];
        foreach ($list as $value) {
            $timezone = $value['timezone'];
            $date = new \DateTime($value['value'], $timezone);
            $date->setTimezone(new \DateTimeZone('UTC'));

            $range = [$date->format('Y-m-d H:i:s'), $date->modify($interval)->format('Y-m-d H:i:s')];
            $cacheKey = md5(json_encode($range));
            if (!isset($cache[$cacheKey])) {
                $params[":{$special->getTableName()}_start"] = $range[0];
                $params[":{$special->getTableName()}_end"] = $range[1];

                $sql = "SELECT $group,{$aggregator->getQuery()} FROM {$this->getParent()->getTableName()} " .
                    "INNER JOIN {$special->getTableName()} ON {$special->getTableName()}.id={$this->getParent()->getTableName()}.{$special->getTableName()}_id " .
                    "WHERE {$special->getTableName()}.{$this->valueField()} >= :{$special->getTableName()}_start AND {$special->getTableName()}.{$this->valueField()} < :{$special->getTableName()}_end " .
                    "GROUP BY $group";
                $cache[$cacheKey] = $this->db()->fetchAll($sql, $params);
            }
            $data = $cache[$cacheKey];
            foreach ($data as $doc) {
                foreach ($doc as $key => $val) {
                    if (substr($key, -3) === '_id') {
                        unset($doc[$key]);
                        $key = substr($key, 0, -3);
                        $doc[$key] = $val;
                    }
                }
                $doc[$dimension->getTableName()] = $value["{$dimension->getTableName()}_id"];
                $this->pushData($doc);
            }
        }
        date_default_timezone_set($currentTZ);
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