<?php

namespace OLAP\DB\SpecialFact\Timezone;

use Doctrine\DBAL\Connection;
use OLAP\DB\SpecialFact\Timezone;

/**
 * Class Dimension
 * @package OLAP\DB\SpecialFact\Timezone
 * @method Timezone sender()
 * @method Dimension getParent()
 */
class Dimension extends \OLAP\DB\Dimension {

    /**
     * @var array
     */
    static private $timezones = [];

    public function getIds(array $data) {

        $special = $this->sender()->getSpecialDimension()->object();
        $value = strtotime($special->mapValue($data)); // datetime in UTC

        $values = [];
        $dateTime = new \DateTime();
        $dateTime->setTimestamp($value);
        foreach ($this->getTimezones() as $tz) {
            $dateTime->setTimezone(new \DateTimeZone($tz));
            $values[$dateTime->format($this->object()->getOption('db-format'))] = true;
        }

        $ids = $this->getIdsByValues(array_keys($values));

        $select = ["{$this->getTableName()}.id as {$this->getTableName()}_id", "{$this->getTableName()}.{$this->sender()->valueField()} as {$this->sender()->valueField()}"];
        $join = [];
        $dimension = $this;
        while ($parent = $dimension->getParent()) {
            $select[] = "{$parent->getTableName()}.id as {$parent->getTableName()}_id";
            if (!$parent->getParent()) { // timezone
                $select[] = "{$parent->getTableName()}.value as timezone";
            }
            $join[] = "INNER JOIN {$parent->getTableName()} ON {$parent->getTableName()}.id={$dimension->getTableName()}.{$parent->getTableName()}_id";
            $dimension = $parent;
        }
        $select = implode(',', $select);
        $join = implode(' ', $join);
        $sql = "SELECT $select FROM {$this->getTableName()} $join WHERE {$this->getTableName()}.id IN (?)";
        return $this->db()->executeQuery($sql, [$ids], [Connection::PARAM_INT_ARRAY])->fetchAll();
    }

    protected function getTimezones() {

        if (empty(self::$timezones)) {

            $dimension = $this;
            while($parent = $dimension->object()->getParent()) {
                $dimension = $this->sender()->getDimension($parent);
            }
            $list = $this->db()->fetchAll("SELECT * FROM {$dimension->getTableName()}");
            if (empty($list)) {
                $params = [];
                foreach (timezone_identifiers_list() as $zone) {
                    $params[] = $zone;
                }
                $sql = "INSERT INTO {$dimension->getTableName()} ({$this->sender()->valueField()}) VALUES(" . implode('),(', array_fill(0, count($params), '?')) . ") RETURNING *";
                $list = $this->db()->fetchAll($sql, $params);
            }
            self::$timezones = [];
            foreach ($list as $row) {
                self::$timezones[$row['id']] = $row[$this->sender()->valueField()];
            }

        }
        return self::$timezones;
    }

    protected function getIdsByValues($values) {

        $filter = [];
        foreach ($values as $value) {
            $date = new \DateTime($value);
            $filter[$date->format($this->object()->getOption('db-format'))] = true;
        }
        $result = [];
        foreach ($filter as $value => $f) {
            $value = (new \DateTime($value))->format($this->object()->getOption('db-format'));

            $sql = "SELECT id FROM {$this->getTableName()} WHERE {$this->sender()->valueField()} = :value";
            $list = $this->db()->fetchAll($sql, [':value' => $value]);
            if (empty($list)) {
                $parent = $this->getParent();
                $insert = [];
                $params = [];
                $i = 0;
                if ($parent->getParent()) { // not year
                    foreach ($parent->getIdsByValues([$value]) as $id) {
                        $insert[] = "(:value_$i, :{$parent->getTableName()}_$i)";
                        $params[":value_$i"] = $value;
                        $params[":{$parent->getTableName()}_$i"] = $id;
                        ++$i;
                    }
                } else {
                    foreach ($this->getTimezones() as $id => $tz) {
                        $insert[] = "(:value_$i, :{$parent->getTableName()}_$i)";
                        $params[":value_$i"] = $value;
                        $params[":{$parent->getTableName()}_$i"] = $id;
                        ++$i;
                    }
                }
                $list = $this->db()->fetchAll(
                    "INSERT INTO {$this->getTableName()} ({$this->sender()->valueField()}, {$parent->getTableName()}_id) VALUES" . implode(',',
                        $insert) . ' RETURNING id',
                    $params
                );
            }
            $list = array_map(function ($e) {
                return $e['id'];
            }, $list);
            $result = array_unique(array_merge($result, $list));
        }
        return $result;
    }
}