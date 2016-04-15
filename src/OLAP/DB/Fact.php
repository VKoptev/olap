<?php

namespace OLAP\DB;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use OLAP\DB\SpecialFact\Timezone;
use OLAP\Event;


/**
 * Class Cube
 * @package OLAP\DB
 * @method \OLAP\Fact object()
 * @method Cube sender()
 */
class Fact extends Base
{

    /**
     * @var Dimension[]
     */
    private $dimensions = [];

    /**
     * @param Connection $db
     * @param \OLAP\Fact $object
     * @param Cube $sender
     */
    public function __construct(Connection $db, \OLAP\Fact $object, Cube $sender = null)
    {

        parent::__construct($db, $object, $sender);

        $this->dimensions = [];
        foreach ($this->object()->getDimensions() as $dimension) {
            $class = $this->dimensionClass();
            $this->dimensions[$dimension->getName()] = new $class($this->db(), $dimension, $this);
        }

        if ($parent = $this->getParent()) {
            Event\Ruler::getInstance()->addListener(new Event\Listener(Event\Type::EVENT_SET_DATA,
                [$this, 'onParentSetData'], $parent->getTableName()));
            Event\Ruler::getInstance()->addListener(new Event\Listener(Event\Type::EVENT_TRUNCATE_FACT,
                [$this, 'onTruncate'], $parent->getTableName()));
        }
    }

    /**
     * @return Dimension[]
     */
    public function getDimensions()
    {

        return $this->dimensions;
    }

    /**
     * @return Type
     */
    public function getDataType()
    {

        return $this->sender()->getDataType();
    }

    /**
     * Check structure of cube
     */
    public function checkStructure()
    {
        foreach ($this->getDimensions() as $dimension) {
            $dimension->checkStructure();
        }

        parent::checkStructure();

        $this->checkSetter();
    }

    public function onTruncate($args)
    {
        $this->truncate();
    }

    public function truncate()
    {
        Event\Ruler::getInstance()->trigger(Event\Type::EVENT_TRUNCATE_FACT, $this->getTableName());
        $this->db()->exec("TRUNCATE {$this->getTableName()} CASCADE");
        foreach ($this->getDimensions() as $dimension) {
            $dimension->truncate();
        }
    }

    /**
     * @return string
     */
    public function dataField()
    {
        return $this->sender()->dataField();
    }

    /**
     * @return string
     */
    public function valueField()
    {
        return $this->sender()->valueField();
    }

    /**
     * @param $name
     * @return Dimension
     */
    public function getDimension($name)
    {
        return empty($this->dimensions[$name]) ? false : $this->dimensions[$name];
    }

    public function setData(array &$data)
    {
        $setter = $this->sender()->getPushData($data);
        $values = [$setter->getQuery()];
        $params = $setter->getParams();
        if ($parent = $this->getParent()) {
//            $values[] = ":{$parent->getTableName()}_id";
//            $params[":{$parent->getTableName()}_id"] = $data["{$parent->getTableName()}_id"];
            $values[] = $data["{$parent->getTableName()}_id"];
        }
        foreach ($this->getStrictDimensions() as $dimension) {
            while($dimension) {
                $key = ":{$dimension->getTableName()}_value";
                $params[$key] = $dimension->mapValue($data);
                $values[] = $key;
                $dimension = $dimension->getParent();
            }
        }

        $setValues = implode(', ', $values);
        $values[0] = 'null';
        $idValues = implode(', ', $values);
        $data["{$this->getTableName()}_id"] = "{$this->setterFunctionName()}($idValues)";
        $data["__sql"][] = "SELECT * FROM {$this->setterFunctionName()}($setValues); ";
        $data["__params"] = array_merge(empty($data['__params']) ? [] : $data['__params'], $params);
//        $data["{$this->getTableName()}_id"] = $this->db()->fetchColumn("SELECT * FROM {$this->setterFunctionName()}($values)", $params);

        Event\Ruler::getInstance()->trigger(Event\Type::EVENT_SET_DATA, $this->getTableName(), ['data' => &$data]);
    }

    public function onParentSetData(&$args)
    {
        if (!empty($args['data'])) {
            $this->setData($args['data']);
        }
    }

    /**
     * @return bool
     */
    public function isDefaultDrill()
    {
        return $this->object()->isDefaultDrill();
    }

    /**
     * @param string $drill
     * @return bool
     */
    public function isDrill($drill)
    {
        return $this->object()->isDrill($drill);
    }

    public function get($filter)
    {
        $params = [];
        $where = [];
        $joins = [];
        $used = [];

        $this->fillGetKeys($filter, $used, $joins, $where, $params);
        $this->fillParentKeys($this->getDimensions(), $filter, $used, $joins, $where, $params);

        $where = empty($where) ? '' : "WHERE (" . implode(') AND (', $where) . ')';
        $getter = $this->sender()->getAggregate($this->getTableName());

        $sql = "SELECT {$getter->getQuery()} FROM {$this->getTableName()} " . implode(' ', $joins) . " $where";

        return $this->db()->fetchAll($sql, array_merge($getter->getParams(), $params));
    }

    /**
     * ===================== SET =====================
     */

    /**
     * @inheritdoc
     */
    protected function createTable()
    {
        list($fields, $constraints) = $this->getTableDefinition();

        $fields = implode(",", $fields);
        $constraints = implode(",", $constraints);

        $this->db()->exec("CREATE TABLE public.{$this->getTableName()} ($fields, $constraints) WITH(OIDS=FALSE);");
    }

    protected function dimensionClass()
    {
        return Dimension::class;
    }

    protected function addFactId()
    {
        // virtual
        return true;
    }

    protected function checkTable()
    {
        list($fields, $constraints) = $this->getTableDefinition();
        $table = $this->describeTable();

        unset($fields['id'], $fields[$this->dataField()], $table['id'], $table[$this->dataField()]);

        $map = array_keys($fields);
        $map = array_combine($map, array_map(function ($e) {
                return $e . '_id';
            }, $map));
        $new = array_diff($map, array_keys($table));
        $fields = array_intersect_key($fields, $new);
        $constraints = array_intersect_key($constraints, $new);
        if (!empty($fields)) {

            $fields = 'ADD COLUMN ' . implode(", ADD COLUMN ", $fields);
            $constraints = 'ADD ' . implode(", ADD ", $constraints);
            $sql = "ALTER TABLE public.{$this->getTableName()} $fields, $constraints";
            $this->db()->exec($sql);
        }
    }

    /**
     * @return Fact
     */
    protected function getParent()
    {
        if ($parent = $this->object()->getParent()) {
            $parent = $this->sender()->getFact($parent);
        }
        return $parent ?: null;
    }

    protected function getTableDefinition()
    {
        $fields = [
            'id' => "id serial NOT NULL",
            $this->dataField() => "{$this->dataField()} {$this->getDataType()->getTableName()}",
        ];
        $constraints = [
            'id' => "CONSTRAINT {$this->getTableName()}_pkey PRIMARY KEY (id)"
        ];
        $key = $this->getKeys();
        foreach ($key as $table => $field) {

            $fields[$table] = "$field integer";
            $constraints[$table] = $this->getFK($table);
        }

        $key = implode(",", $key);
        $constraints['unique'] = "CONSTRAINT {$this->valueConstraint()} UNIQUE ($key)";

        return [$fields, $constraints];
    }

    protected function valueConstraint()
    {
        return "{$this->getTableName()}_unique";
    }


    protected function setterFunctionName()
    {
        return "get_{$this->getTableName()}_id";
    }

    /**
     * Check push fact value function existence
     */
    protected function checkSetter()
    {
        $params = [$this->getDataType()->getTableName()];
        $fields = [$this->sender()->dataField() => '$1'];
        $declare = ["DECLARE result integer; "];
        $setVars = [];
        $i = 1; // value

        if ($this->addFactId() && $parent = $this->getParent()) {
            $params[] = "integer";
            $fields["{$parent->getTableName()}_id"] = '$2';
            $i++;
        }

        foreach ($this->getStrictDimensions() as $dimension) {
            $this->addDimension($dimension, $i, $params, $declare, $fields, $setVars);
        }

        $whereValues = [];
        foreach ($fields as $key => $value) {
            if ($key !== $this->sender()->dataField()) {
                $whereValues[] = "public.{$this->getTableName()}.$key = $value";
            }
        }
        $whereValues = implode(' AND ', $whereValues);
        $insertValues = implode(', ', $fields);
        $fields = implode(', ', array_keys($fields));

        $params = implode(', ', $params);
        $declare = implode(' ', $declare);
        $setVars = implode(' ', $setVars);


        $pushMethod = $this->sender()->getPushMethod('$1', "public.{$this->getTableName()}");
        $sql = $this->getSetterSql($params, $declare, $setVars, $fields, $insertValues, $pushMethod, $whereValues);

        $this->db()->exec($sql);
    }

    /**
     * Add dimension to function definition
     * @param Dimension $dimension
     * @param $i
     * @param $params
     * @param $declare
     * @param $fields
     * @param $setVars
     * @param bool|true $key
     */
    protected function addDimension(Dimension $dimension, &$i, &$params, &$declare, &$fields, &$setVars, $key = true)
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
            $setVars[] = "SELECT * INTO {$dimension->getTableName()}_id_var FROM get_{$dimension->getTableName()}_id({$parentParams}); ";
        }

        if ($parent = $dimension->getParent()) {
            $this->addDimension($parent, $i, $params, $declare, $fields, $setVars, false);
        }
    }

    protected function getSetterSql($params, $declare, $setVars, $fields, $insertValues, UserQuery $pushMethod, $whereValues)
    {
        return "CREATE OR REPLACE FUNCTION {$this->setterFunctionName()}({$params}) " . "RETURNS integer AS $$ " . $declare . "BEGIN $setVars " .
        "UPDATE public.{$this->getTableName()} SET {$this->sender()->dataField()} = {$pushMethod->getQuery()} WHERE {$whereValues} RETURNING id INTO result; " .
        "IF result IS NULL THEN " .
        "INSERT INTO public.{$this->getTableName()} ($fields) VALUES($insertValues) " .
        "ON CONFLICT ON CONSTRAINT {$this->valueConstraint()} " .
        "DO UPDATE SET {$this->sender()->dataField()} = {$pushMethod->getQuery()} WHERE {$whereValues} " .
        "RETURNING id INTO result; " .
        "END IF; " .
        "RETURN result; " . "END; " . "$$ LANGUAGE plpgsql;";
    }

    protected function getKeys()
    {
        $result = [];
        foreach ($this->getStrictDimensions() as $dimension) {
            $result[$dimension->getTableName()] = "{$dimension->getTableName()}_id";
        }

        if ($this->addFactId() && $parent = $this->getParent()) {
            $result[$parent->getTableName()] = "{$parent->getTableName()}_id";
        }
        return $result;
    }

    /**
     * @return Dimension[]
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
        return array_diff_key($result, $parents);
    }

    /**
     * ===================== GET =====================
     */

    /**
     * @param $field
     * @param $value
     * @param $where
     * @param $params
     */
    protected function pushWhere($field, $value, &$where, &$params)
    {
        $paramKey = ':' . str_replace('.', '_', $field);
        if (is_array($value)) {
            if ($value === array_values($value)) { // array
                $this->pushOperator($field, '$in', $value, $where, $params);
            } else {
                foreach ($value as $operator => $value) {
                    $this->pushOperator($field, $operator, $value, $where, $params);
                }
            }
        } else {
            $where[] = "$field = $paramKey";
            $params[$paramKey] = $value;
        }
    }

    protected function pushOperator($field, $operator, $value, &$where, &$params)
    {
        $paramKey = ':' . str_replace('.', '_', $field);
        switch ($operator) {
            case '$in':
                $list = [];
                $i = 0;
                foreach ($value as $val) {
                    $list[] = "{$paramKey}_$i";
                    $params["{$paramKey}_$i"] = $val;
                    ++$i;
                }
                $list = implode(',', $list);
                $where[] = "$field IN ($list)";
                break;
            case '$gt':
            case '$lt':
            case '$gte':
            case '$lte':
                $i = 0;
                $key = $paramKey;
                while (isset($params[$key])) {
                    $key = "{$paramKey}_$i";
                    ++$i;
                }
                $paramKey = $key;
                $op = $operator{1} == 'l' ? '<' : '>';
                if (strlen($operator) > 3) {
                    $op .= '=';
                }
                $where[] = "$field $op $paramKey";
                $params[$paramKey] = $value;
                break;
        }
    }

    protected function fillGetKeys($filter, &$used, &$joins, &$where, &$params)
    {
        foreach ($this->getKeys() as $dimensionName => $dimensionId) {

            $dimension = $this->getDimension($dimensionName);
            $value = $dimension->mapValue($filter);
            if ($value !== null) {
                $used[$dimensionName] = true;
                $joins[$dimensionName] = "INNER JOIN {$dimensionName} ON {$dimensionName}.id = {$this->getTableName()}.{$dimensionId}";
                $this->pushWhere("{$dimensionName}.{$this->valueField()}", $value, $where, $params);
            }
        }
    }

    /**
     * @param Dimension[] $dimensions
     * @param array $filter
     * @param array $used
     * @param array $joins
     * @param array $where
     * @param array $params
     */
    protected function fillParentKeys($dimensions, $filter, &$used, &$joins, &$where, &$params)
    {
        foreach ($dimensions as $dimensionName => $dimension) {
            $value = $dimension->mapValue($filter);
            if (empty($used[$dimensionName]) && $value !== null) {
                // there are only parents
                $used[$dimensionName] = true;
                $this->pushWhere("{$dimensionName}.{$this->valueField()}", $value, $where, $params);
                $this->fillParentKeysByDimension($dimension, $dimensions, $used, $joins, $where, $params);
            }
        }
    }

    /**
     * @param Dimension $parent
     * @param Dimension[] $dimensions
     * @param array $used
     * @param array $joins
     * @param array $where
     * @param array $params
     */
    protected function fillParentKeysByDimension($parent, $dimensions, &$used, &$joins, &$where, &$params)
    {
        $keys = $this->getKeys();
        foreach ($dimensions as $dimensionName => $dimension) {

            if ($dimension->object()->getParent() === $parent->getTableName()) {
                if (empty($used[$dimensionName])) {
                    $used[$dimensionName] = true;
                    if (isset($keys[$dimensionName])) {
                        $joins[$dimensionName] = "INNER JOIN {$dimensionName} ON {$dimensionName}.id = {$this->getTableName()}.{$dimensionName}_id";;
                    } else {
                        $this->fillParentKeysByDimension($dimension, $dimensions, $used, $joins, $where, $params);
                    }
                }
                $joins[$parent->getTableName()] = "INNER JOIN {$parent->getTableName()} ON {$parent->getTableName()}.id={$dimensionName}.{$parent->getTableName()}_id";
            }
        }
    }
}