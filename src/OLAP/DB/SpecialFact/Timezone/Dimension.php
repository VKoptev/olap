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
class Dimension extends \OLAP\DB\Dimension
{

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

    public function __construct(Connection $db, \OLAP\Dimension $object, Timezone $sender = null)
    {
        parent::__construct($db, $object, $sender);
        $this->format = $this->object()->getOption('db-format', 'Y-m-d');
    }

    public function truncate()
    {
        parent::truncate();
        if (!$this->getParent()) {
            // add timezones
            $params = array_keys($this->nativeTZList());
            $sql = "INSERT INTO {$this->getTableName()} ({$this->sender()->valueField()}) VALUES(" . implode('),(',
                    array_fill(0, count($params), '?')) . ")";
            $this->db()->fetchAll($sql, $params);
        }
    }

    /**
     * @param array $data
     * @return mixed
     */
    public function mapValue($data)
    {
        $value = $this->sender()->getSpecialDimension()->mapValue($data);
        return (new \DateTime($value))->format($this->format);
    }

    /**
     * @return array
     */
    protected function nativeTZList()
    {
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


    protected function checkSetter()
    {
        if ($parent = $this->getParent()) {
            $sql = "CREATE OR REPLACE FUNCTION get_{$this->getTableName()}_id(date) RETURNS setof integer " .
                "AS $$ " .
                "DECLARE r integer; BEGIN " .
                "FOR r IN SELECT * FROM get_{$parent->getTableName()}_id(\$1) LOOP " .
		            "INSERT INTO public.{$this->getTableName()} (value, {$parent->getTableName()}_id) VALUES(date_trunc('{$this->getTrunc()}', \$1), r) " .
			        "ON CONFLICT ON CONSTRAINT {$this->valueConstraint()} DO UPDATE SET id = public.{$this->getTableName()}.id " .
			        "RETURNING id INTO r; " .
		            "RETURN NEXT r; " .
	            "END LOOP; " .
                "END; $$ LANGUAGE plpgsql;";
        } else {
            $sql = "CREATE OR REPLACE FUNCTION get_{$this->getTableName()}_id(date) RETURNS setof integer " .
                "AS $$ SELECT id FROM public.{$this->getTableName()} $$ LANGUAGE SQL;";
        }
        $this->db()->exec($sql);
    }

    public function getSetterParamsCount()
    {
        return 1;
    }

    private function getTrunc()
    {
        switch ($this->format) {
            case 'Y-01-01':
                return 'year';
            case 'Y-m-01':
                return 'month';
            case 'Y-m-d':
                return 'day';
            default:
                return 'second';
        }
    }
}