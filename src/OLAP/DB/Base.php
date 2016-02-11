<?php
namespace OLAP\DB;


use Doctrine\DBAL\Connection;
use OLAP\Model;

abstract class Base {

    /**
     * @var Connection
     */
    private $db;
    /**
     * @var Model
     */
    private $object;

    /**
     * @var Base
     */
    private $sender;

    /**
     * @param Connection $db
     * @param mixed $object
     * @param Base $sender
     */
    public function __construct(Connection $db, Model $object, Base $sender = null) {

        $this->db = $db;
        $this->object = $object;
        $this->sender = $sender;
    }

    public function checkStructure() {

        if ($this->tableExists($this->getTableName())) {
            $this->checkTable();
        } else {
            $this->createTable();
        }
    }

    public function getTableName() {

        return $this->object()->getName();
    }

    /**
     * @return Connection
     */
    protected function db() {

        return $this->db;
    }

    /**
     * @return Model
     */
    protected function object() {

        return $this->object;
    }

    /**
     * @return Base
     */
    protected function sender() {

        return $this->sender;
    }

    protected function tableExists($table) {

        return (bool)$this->db()->fetchColumn("SELECT to_regclass('public.$table')");
    }

    protected function getFK($table) {

        return
            "CONSTRAINT {$this->getTableName()}_{$table}_id_fkey FOREIGN KEY ({$table}_id) " .
            "REFERENCES {$table} (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION";
    }

    protected function describeTable() {

        $columns = [];
        $cursor = $this->db()->fetchAll("SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_name = :table", [':table' => $this->getTableName()]);
        foreach ($cursor as $row) {
            $columns[$row['column_name']] = strtolower($row['data_type']) === 'user-defined' ? $row['udt_name'] : $row['data_type'];
        }
        return $columns;
    }

    protected function createTable() {

        // virtual
    }

    protected function checkTable() {

        // virtual
    }
}