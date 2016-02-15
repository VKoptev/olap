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

        $value = $this->object()->mapValue($data);
        if ($value === null) {
            $id = $this->db()->fetchColumn("SELECT id FROM public.{$this->getTableName()} WHERE {$this->sender()->valueField()} IS NULL");
        } else {
            $id = $this->db()->fetchColumn(
                "SELECT id FROM public.{$this->getTableName()} WHERE {$this->sender()->valueField()} = :value",
                [':value' => $value]
            );
        }
        if (empty($id)) {
            $values = [$this->sender()->valueField() => ':value'];
            $params = [':value' => $value];

            if ($parent = $this->getParent()) {
                $pid = "{$parent->getTableName()}_id";
                $values[$pid] = ":$pid";
                $params[":$pid"] = $parent->getId($data);
            }

            $fields = implode(',', array_keys($values));
            $values = implode(',', array_values($values));
            $id = $this->db()->fetchColumn(
                "INSERT INTO public.{$this->getTableName()} ($fields) VALUES ($values) RETURNING id",
                $params
            );
        }
        return $id;
    }

    protected function createTable() {

        $this->getType()->checkStructure();

        $fields = [
            "id serial NOT NULL",
            "{$this->sender()->valueField()} {$this->getType()->getTableName()}"
        ];
        $constraints    = [
            "CONSTRAINT {$this->getTableName()}_pkey PRIMARY KEY (id)"
        ];
        if ($this->object()->isDenormalized()) {
            $fields[] = $this->sender()->dataField() . ' ' . $this->sender()->getDataType()->getTableName();
        }
        if ($parent = $this->object()->getParent()) {
            $parent = $this->sender()->getDimension($parent);
            $fields[] = "{$parent->getTableName()}_id integer";
            $constraints[] = $this->getFK($parent->getTableName());
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
}