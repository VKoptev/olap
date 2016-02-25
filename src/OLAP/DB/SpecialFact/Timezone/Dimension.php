<?php

namespace OLAP\DB\SpecialFact\Timezone;

use Doctrine\DBAL\Connection;
use OLAP\DB\SpecialFact\Timezone;
use OLAP\Exception;

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
    /**
     * @var array
     */
    static private $nativeTZ = [];

    /**
     * @var string
     */
    private $format;

    public function __construct(Connection $db, \OLAP\Dimension $object, Timezone $sender = null) {

        parent::__construct($db, $object, $sender);
        $this->format = $this->object()->getOption('db-format', 'Y-m-d');
    }

    public function truncate() {

        parent::truncate();
        if (!$this->getParent()) {
            // add timezones
            $params = array_keys($this->nativeTZList());
            $sql = "INSERT INTO {$this->getTableName()} ({$this->sender()->valueField()}) VALUES("
                 . implode('),(', array_fill(0, count($params), '?')) . ")";
            $this->db()->fetchAll($sql, $params);
        }
    }

    public function getIds(array $data) {

        $special = $this->sender()->getSpecialDimension()->object();
        $value = strtotime($special->mapValue($data)); // datetime in UTC

        $values = [];
        $dateTime = new \DateTime();
        $dateTime->setTimestamp($value);
        foreach ($this->getTimezones() as $tz) {
            $tz = $this->getTimezoneByOffset($tz);
            $dateTime->setTimezone($tz);
            $values[$dateTime->format($this->format)] = true;
        }
        $result = $this->getInfo(array_keys($values));
        foreach ($result as $i => &$row) {
            $dateTime->setTimezone($row['timezone']);
            $current = new \DateTime($row['value'], $row['timezone']);
            if ($current->format($this->format) !== $dateTime->format($this->format)) {
                unset($result[$i]);
            }
        }

        return array_values($result);
    }

    /**
     * @param array $data
     * @return mixed
     */
    public function mapValue($data) {

        $result = $this->object()->mapValue($data);
        if ($result && $this->getParent()) {
            if (is_array($result)) {
                foreach ($result as &$el) {
                    $el = (new \DateTime($el))->format($this->format);
                }
            } else {
                $result = (new \DateTime($result))->format($this->format);
            }
        }
        return $result;
    }

    protected function getTimezones() {

        if (empty(self::$timezones)) {

            $dimension = $this;
            while($parent = $dimension->object()->getParent()) {
                $dimension = $this->sender()->getDimension($parent);
            }
            $list = $this->db()->fetchAll("SELECT * FROM {$dimension->getTableName()}");
            if (empty($list)) {
                throw new Exception('Truncate automatic!');
            }
            self::$timezones = [];
            foreach ($list as $row) {
                self::$timezones[$row['id']] = $row[$this->sender()->valueField()];
            }

        }
        return self::$timezones;
    }

    /**
     * @return array
     */
    protected function nativeTZList() {

        if (empty(self::$nativeTZ)) {
            self::$nativeTZ = [];
            foreach (timezone_identifiers_list() as $zone) {
                $zone = new \DateTimeZone($zone);
                $key = floor($zone->getOffset(new \DateTime()) / 3600) * 3600;
                self::$nativeTZ[$key] = $zone;
            }
        }
        return self::$nativeTZ;
    }

    protected function getInfo($values) {

        $ids = $this->getIdsByValues($values);

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
        $result = $this->db()->executeQuery($sql, [$ids], [Connection::PARAM_INT_ARRAY])->fetchAll();
        foreach ($result as &$row) {
            $row['timezone'] = $this->getTimezoneByOffset($row['timezone']);
        }
        return $result;
    }

    protected function getIdsByValues($values, $returnValues = false) {

        $filter = [];
        foreach ($values as $value) {
            $date = new \DateTime($value);
            $filter[$date->format($this->format)] = true;
        }
        $filter = array_keys($filter);
        if (empty($filter)) {
            return [];
        }

        $parentIds = [];
        if ($this->getParent()->getParent()) {
            $parentIds = $this->getParent()->getIdsByValues($filter, true);
        } else {
            $parentIds = array_keys($this->getTimezones());
        }
        $values = [];
        $where  = [];
        $i = 0;
        $year = !$this->getParent()->getParent();
        foreach ($filter as $value) {
            $valueInParent = (new \DateTime($value))->format($this->getParent()->format);
            foreach ($parentIds as $id => $pvalue) {
                // no params because type cast
                if ($year) {
                    $id = $pvalue;
                }
                if ($year || $valueInParent === $pvalue) {
                    $values[] = "('$value'::date, $id)";
                    $where[$value] = "'$value'::date";
                    ++$i;
                }
            }
        }

        $where = implode(',', $where);
        $fields = "{$this->sender()->valueField()}, {$this->getParent()->getTableName()}_id";
        $sql = "WITH new_data as (INSERT INTO public.{$this->getTableName()} ($fields) VALUES " . implode(',', $values) .
               " ON CONFLICT ON CONSTRAINT {$this->valueConstraint()} DO NOTHING RETURNING id, {$this->sender()->valueField()}) " .
               "SELECT id, {$this->sender()->valueField()} as value FROM new_data " .
               "UNION SELECT id, {$this->sender()->valueField()} as value FROM public.{$this->getTableName()} WHERE {$this->sender()->valueField()} IN ($where)"
        ;

        $result = [];
        foreach ($this->db()->fetchAll($sql) as $row) {
            $result[$row['id']] = $row['value'];
        }
        return $returnValues ? $result : array_keys($result);
    }

    private function getTimezoneByOffset($offset) {

        return $this->nativeTZList()[$offset];
    }
}