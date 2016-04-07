<?php

namespace OLAP\DB;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Sharding\SQLAzure\SQLAzureFederationsSynchronizer;

/**
 * Class Dimension
 * @package OLAP\DB
 * @method \OLAP\Dimension object()
 * @method Fact sender()
 */
class Dimension extends Base
{

    /**
     * @var Type
     */
    private $type;

    /**
     * @param Connection $db
     * @param mixed $object
     * @param Fact $sender
     */
    public function __construct(Connection $db, \OLAP\Dimension $object, Fact $sender = null)
    {
        parent::__construct($db, $object, $sender);
        $this->type = new Type($this->db(), $this->object()->getType(), $this);

    }

    /**
     * @return Type
     */
    public function getType()
    {
        return $this->type;
    }

    public function checkStructure()
    {
        parent::checkStructure();

        $this->checkSetter();
    }

    public function truncate()
    {
        $this->db()->exec("TRUNCATE {$this->getTableName()} CASCADE");
    }

    /**
     * @param array $data
     * @return mixed
     */
    public function mapValue($data)
    {
        return $this->object()->mapValue($data);
    }

    /**
     * @return Dimension
     */
    public function getParent()
    {
        if ($parent = $this->object()->getParent()) {
            $parent = $this->sender()->getDimension($parent);
        }
        return $parent ?: null;
    }

    public function getSetterParamsCount()
    {
        return 1 + ($this->getParent() ? $this->getParent()->getSetterParamsCount() : 0);
    }

    protected function checkSetter()
    {
        $params = ["{$this->sender()->valueField()} {$this->getType()->getTableName()}"];
        $fields = [$this->sender()->valueField()];
        $values = ["{$this->getTableName()}.{$this->sender()->valueField()}" => '$1'];
        $declare = ["DECLARE result integer; "];
        $setVars = "";

        if ($parent = $this->getParent()) {
            $parentParams = [];
            for ($i = 2; $i <= $this->getSetterParamsCount(); $i++) {
                $parentParams[] = "\${$i}";
            }
            $parentParams = implode(', ', $parentParams);

            $fields[] = "{$parent->getTableName()}_id";
            $values["{$this->getTableName()}.{$parent->getTableName()}_id"] = "{$parent->getTableName()}_id_var";
            $declare[] = "DECLARE {$parent->getTableName()}_id_var integer; ";
            $setVars .= "SELECT * INTO {$parent->getTableName()}_id_var FROM get_{$parent->getTableName()}_id({$parentParams}); ";

            $ptr = $this;
            while ($parent = $ptr->getParent()) {
                $params[] = "{$parent->getTableName()}_value {$parent->getType()->getTableName()}";
                $ptr = $parent;
            }
        }

        $params = implode(', ', $params);
        $fields = implode(', ', $fields);
        $declare = implode(' ', $declare);
        $insertValues = implode(', ', $values);
        $whereValues = [];
        foreach ($values as $key => $value) {
            $whereValues[] = "$key = $value";
        }
        $whereValues = implode(' AND ', $whereValues);

        $sql = "CREATE OR REPLACE FUNCTION get_{$this->getTableName()}_id({$params}) " .
            "RETURNS integer AS $$ " . $declare . "BEGIN $setVars " .
            "WITH new_row as ( " .
            "INSERT INTO public.{$this->getTableName()} ($fields) VALUES($insertValues) " .
            "ON CONFLICT ON CONSTRAINT {$this->valueConstraint()} DO NOTHING " .
            "RETURNING id " . ") SELECT x.id INTO result FROM ( " .
            "SELECT id FROM new_row UNION " .
            "SELECT id FROM public.{$this->getTableName()} WHERE {$whereValues} " . ") x; " . "RETURN result; " . "END; " . "$$ LANGUAGE plpgsql;";

        $this->db()->exec($sql);
    }

    protected function createTable()
    {
        $this->getType()->checkStructure();

        $fields = [
            "id serial NOT NULL",
            "{$this->sender()->valueField()} {$this->getType()->getTableName()}"
        ];
        $constraints = [
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
        $fields = implode(",", $fields);
        $constraints = implode(",", $constraints);

        $sql = "CREATE TABLE public.{$this->getTableName()} ($fields, $constraints) WITH(OIDS=FALSE);" . "CREATE INDEX {$this->getTableName()}_{$this->sender()->valueField()}_idx ON public.{$this->getTableName()} " . "USING {$this->object()->getIndex()}({$this->sender()->valueField()});";

        $this->db()->exec($sql);
    }

    protected function valueConstraint()
    {
        return "{$this->getTableName()}_{$this->sender()->valueField()}_uniq";
    }
}