<?php

namespace OLAP\DB\SpecialFact;


use Doctrine\DBAL\Connection;
use OLAP\DB\Cube;
use OLAP\DB\UserQuery;
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

        $setter = $this->sender()->getPushData($data);
        $values = [$setter->getQuery()];
        $params = $setter->getParams();
        foreach ($this->getStrictDimensions() as $dimension) {
            while($dimension) {
                $key = ":{$dimension->getTableName()}_value";
                $params[$key] = $dimension->mapValue($data);
                $values[] = $key;
                if ($dimension->getTableName() === $this->getDimension('dimension_day')->getTableName()) {
                    $dimension = null;
                } else {
                    $dimension = $dimension->getParent();
                }
            }
        }

        $values = implode(', ', $values);
        $this->db()->beginTransaction();
        $data["{$this->getTableName()}_id"] = $this->db()->fetchColumn("SELECT * FROM {$this->setterFunctionName()}($values)", $params);
        $this->db()->commit();

        Event\Ruler::getInstance()->trigger(Event\Type::EVENT_SET_DATA, $this->getTableName(), ['data' => $data]);

        date_default_timezone_set($currentTZ);
    }

    protected function dimensionClass() {

        return Dimension::class;
    }

    protected function addFactId() {

        return false;
    }

    protected function addDimension(\OLAP\DB\Dimension $dimension, &$i, &$params, &$declare, &$fields, &$setVars, $key = true)
    {
        $i++;
        $params[] = $dimension->getType()->getTableName();
        if ($key) {
            $declare[] = "DECLARE {$dimension->getTableName()}_id_var integer; ";
            $fields["{$dimension->getTableName()}_id"] = "{$dimension->getTableName()}_id_var";
            $parentParams = [];
            for ($j = 0; $j < $dimension->getSetterParamsCount(); $j++) {
                $parentParams[] = '$' . ($j + $i);
            }
            $parentParams = implode(', ', $parentParams);
            if ($this->getParent()->getDimension($dimension->getTableName())) {
                $setVars[] = "SELECT * INTO {$dimension->getTableName()}_id_var FROM get_{$dimension->getTableName()}_id({$parentParams}); ";
            }
        }

        if (($parent = $dimension->getParent()) && $dimension->getTableName() != $this->getDimension('dimension_day')->getTableName()) {
            $this->addDimension($parent, $i, $params, $declare, $fields, $setVars, false);
        }
    }

    protected function getSetterSql($params, $declare, $setVars, $fields, $insertValues, UserQuery $pushMethod, $whereValues)
    {
        $listDimension = $this->getDimension('dimension_day');

        return "CREATE OR REPLACE FUNCTION {$this->setterFunctionName()}({$params}) " . "RETURNS setof integer AS $$ " . $declare . "BEGIN $setVars " .
            "FOR {$listDimension->getTableName()}_id_var IN SELECT * FROM public.get_{$listDimension->getTableName()}_id($2) LOOP " .
            "INSERT INTO public.{$this->getTableName()} ($fields) VALUES($insertValues) " .
            "ON CONFLICT ON CONSTRAINT {$this->valueConstraint()} " .
            "DO UPDATE SET {$this->sender()->dataField()} = {$pushMethod->getQuery()} WHERE {$whereValues} " .
            "RETURNING id INTO result; " .
            "RETURN NEXT result; END LOOP;" . "END; " . "$$ LANGUAGE plpgsql;";
    }

    /**
     * @inheritdoc
     */
    protected function getStrictDimensions()
    {
        $result = [];
        $parents = [];
        foreach ($this->getDimensions() as $dimension) {
            $result[$dimension->getTableName()] = $dimension;
            if ($parent = $dimension->getParent()) {
                $parents[$parent->getTableName()] = true;
            }
        }
        $result = array_diff_key($result, $parents);
        $parentFactDimensions = array_diff_key($this->getParent()->getStrictDimensions(), [$this->getSpecialDimension()->getTableName() => '']);
        return array_merge($result, $parentFactDimensions);
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