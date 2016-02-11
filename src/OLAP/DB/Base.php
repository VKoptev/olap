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

    protected function createTable() {

        // virtual
    }

    protected function checkTable() {

        // virtual
    }
}