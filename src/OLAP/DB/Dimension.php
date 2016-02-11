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
}