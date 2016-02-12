<?php

namespace OLAP\DB;

use Doctrine\DBAL\Connection;


/**
 * Class Cube
 * @package OLAP\DB
 * @method \OLAP\Fact object()
 * @method Cube sender()
 */
class Fact extends Base {

    /**
     * @var Dimension[]
     */
    private $dimensions = [];

    /**
     * @param Connection $db
     * @param \OLAP\Fact $object
     * @param Base $sender
     */
    public function __construct(Connection $db, \OLAP\Fact $object, Base $sender = null) {

        parent::__construct($db, $object, $sender);

        $this->dimensions = [];
        foreach ($this->object()->getDimensions() as $dimension) {
            $this->dimensions[$dimension->getName()] = new Dimension($this->db(), $dimension, $this);
        }
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

        return $this->sender()->getDataType();
    }

    /**
     * Check structure of cube
     */
    public function checkStructure() {

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

    public function setData(array $data) {

        $where  = [];
        $params = [];
        $fields = $this->getKeys();
        foreach ($this->getDimensions() as $dimension) {
            $key = $dimension->getTableName();
            if (!isset($fields[$key])) {
                // skip parents
                continue;
            }
            $value = $dimension->getId($data);
            $params[":$key"] = $dimension->getId($data);
            $where[] = "$fields[$key] " . ($value === null ? 'IS NULL' : "= :$key");
        }
        $where = $where ? 'WHERE (' . implode(') AND (', $where) . ')' : '';
        $setter = $this->sender()->getSetter($data);

        $id = $this->db()->fetchColumn("SELECT id from public.{$this->getTableName()} $where", $params);
        if (!$id) {
            $values = ':' . implode(',:', array_keys($fields));
            $fields = implode(',', array_values($fields));
            $id = $this->db()->fetchColumn(
                "INSERT INTO public.{$this->getTableName()} ($fields) VALUES ($values) RETURNING id",
                $params
            );
        }
        $params = array_merge($params, $setter->getParams(), [':id' => $id]);
        $this->db()->fetchColumn("UPDATE public.{$this->getTableName()} SET {$setter->getQuery()} WHERE id=:id RETURNING id", $params);
    }

    protected function createTable() {

        list($fields, $constraints) = $this->getTableDefinition();

        $fields         = implode(",", $fields);
        $constraints    = implode(",", $constraints);

        $this->db()->exec("CREATE TABLE public.{$this->getTableName()} ($fields, $constraints) WITH(OIDS=FALSE);");
    }

    protected function checkTable() {

        list($fields, $constraints) = $this->getTableDefinition();
        $table = $this->describeTable();

        unset($fields['id'], $fields[$this->dataField()], $table['id'], $table[$this->dataField()]);

        $map = array_keys($fields);
        $map = array_combine(
            $map,
            array_map(function($e){
                return $e . '_id';
            }, $map)
        );
        $new = array_diff($map, array_keys($table));
        $fields = array_intersect_key($fields, $new);
        $constraints = array_intersect_key($constraints, $new);
        if (!empty($fields)) {

            $fields         = 'ADD COLUMN ' . implode(", ADD COLUMN ", $fields);
            $constraints    = 'ADD ' . implode(", ADD ", $constraints);
            $sql = "ALTER TABLE public.{$this->getTableName()} $fields, $constraints";
            $this->db()->exec($sql);
        }
    }

    private function getTableDefinition() {


        $fields         = [
            'id' => "id serial NOT NULL",
            $this->dataField() => "{$this->dataField()} {$this->getDataType()->getTableName()}",
        ];
        $constraints    = [
            'id' => "CONSTRAINT {$this->getTableName()}_pkey PRIMARY KEY (id)"
        ];
        $key = $this->getKeys();
        foreach ($key as $table => $field) {

            $fields[$table]       = "$field integer";
            $constraints[$table]  = $this->getFK($table);
        }

        if (($parent = $this->object()->getParent()) && ($parent = $this->sender()->getFact($parent))) {
            $fields[$parent->getTableName()] = "{$parent->getTableName()}_id integer";
            $constraints[$parent->getTableName()]  = $this->getFK($parent->getTableName());
        }

        $key = implode(",", $key);
        $constraints['unique'] = "CONSTRAINT {$this->getTableName()}_unique UNIQUE ($key)";

        return [$fields, $constraints];
    }

    private function getKeys() {

        $key     = [];
        $parents = [];
        foreach ($this->getDimensions() as $dimension) {
            $key[$dimension->getTableName()]          = "{$dimension->getTableName()}_id";
            if ($parent = $dimension->object()->getParent()) {
                $parents[$parent] = true;
            }
        }
        $key = array_diff_key($key, $parents);
        return $key;
    }
}