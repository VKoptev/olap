<?php

namespace OLAP\DB;

use Doctrine\DBAL\Connection;

/**
 * Class Dimension
 * @package OLAP\DB
 * @method \OLAP\Dimension object()
 * @method Fact sender()
 */
class Dimension extends Base {

    /**
     * @var Type
     */
    private $type;

    /**
     * @param Connection $db
     * @param mixed $object
     * @param Fact $sender
     */
    public function __construct(Connection $db, \OLAP\Dimension $object, Fact $sender = null) {

        parent::__construct($db, $object, $sender);
        $this->type = new Type($this->db(), $this->object()->getType(), $this);

    }

    /**
     * @return Type
     */
    public function getType() {

        return $this->type;
    }

    public function getId(array $data) {

        $value = $this->mapValue($data);

        if ($value === null) {
            return null;
        }

        $values = [$this->sender()->valueField() => ':value'];
        $params = [':value' => $value];

        if ($parent = $this->getParent()) {
            $pid = "{$parent->getTableName()}_id";
            $values[$pid] = ":$pid";
            $params[":$pid"] = $parent->getId($data);
        }

        $fields = implode(',', array_keys($values));
        $values = implode(',', array_values($values));

        $sql = "WITH new_row as (" .
                    "INSERT INTO public.{$this->getTableName()} ($fields) VALUES($values)" .
                    "ON CONFLICT ON CONSTRAINT {$this->valueConstraint()} DO NOTHING " .
                    "RETURNING id " .
               ") SELECT id FROM new_row UNION ".
               "SELECT id FROM public.{$this->getTableName()} WHERE {$this->sender()->valueField()} = :value"
        ;
        return $this->db()->fetchColumn($sql, $params);
    }

    public function truncate() {

        $this->db()->exec("TRUNCATE {$this->getTableName()} CASCADE");
    }

    /**
     * @param array $data
     * @return mixed
     */
    public function mapValue($data) {

        return $this->object()->mapValue($data);
    }

    protected function createTable() {

        $this->getType()->checkStructure();

        $fields = [
            "id serial NOT NULL",
            "{$this->sender()->valueField()} {$this->getType()->getTableName()}"
        ];
        $constraints    = [
            "CONSTRAINT {$this->getTableName()}_pkey PRIMARY KEY (id)",
        ];
        if ($this->object()->isDenormalized()) {
            $fields[] = $this->sender()->dataField() . ' ' . $this->sender()->getDataType()->getTableName();
        }
        if ($parent = $this->object()->getParent()) {
            $parent = $this->sender()->getDimension($parent);
            $fields[] = "{$parent->getTableName()}_id integer";
            $constraints[] = $this->getFK($parent->getTableName());
            $constraints[] = "CONSTRAINT {$this->valueConstraint()} UNIQUE ({$this->sender()->valueField()}, {$parent->getTableName()}_id)";
        } else {
            $constraints[] = "CONSTRAINT {$this->valueConstraint()} UNIQUE ({$this->sender()->valueField()})";
        }
        $fields         = implode(",", $fields);
        $constraints    = implode(",", $constraints);

        $sql =  "CREATE TABLE public.{$this->getTableName()} ($fields, $constraints) WITH(OIDS=FALSE);" .
                "CREATE INDEX {$this->getTableName()}_{$this->sender()->valueField()}_idx ON public.{$this->getTableName()} " .
                    "USING {$this->object()->getIndex()}({$this->sender()->valueField()});"
        ;

        $this->db()->exec($sql);
    }

    /**
     * @return Dimension
     */
    protected function getParent() {
        if ($parent = $this->object()->getParent()) {
            $parent = $this->sender()->getDimension($parent);
        }
        return $parent ?: null;
    }

    protected function valueConstraint() {

        return "{$this->getTableName()}_{$this->sender()->valueField()}_uniq";
    }
}