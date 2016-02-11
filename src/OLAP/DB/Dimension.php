<?php

namespace OLAP\DB;

use Doctrine\DBAL\Connection;

/**
 * Class Dimension
 * @package OLAP\DB
 * @method \OLAP\Dimension object()
 * @method Cube sender()
 */
class Dimension extends Base {

    /**
     * @var Type
     */
    private $type;

    /**
     * @param Connection $db
     * @param mixed $object
     * @param Cube $sender
     */
    public function __construct(Connection $db, \OLAP\Dimension $object, Cube $sender = null) {

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

        $data = '';
        if ($this->object()->isDenormalized()) {
            $data = $this->sender()->dataField() . ' ' . $this->sender()->getDataType()->getTableName() . ',';
        }

        $table = $this->getTableName();
        $sql = <<<SQL
CREATE TABLE public.{$table} (
  id serial NOT NULL,
  {$this->sender()->valueField()} {$this->getType()->getTableName()},
  $data
  CONSTRAINT {$table}_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
CREATE INDEX {$table}_{$this->sender()->valueField()}_idx
  ON public.{$table}
  USING {$this->object()->getIndex()}
  ({$this->sender()->valueField()});
SQL;

        $this->db()->exec($sql);
    }
}