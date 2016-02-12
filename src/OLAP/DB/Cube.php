<?php

namespace OLAP\DB;

use Doctrine\DBAL\Connection;


/**
 * Class Cube
 * @package OLAP\DB
 * @method \OLAP\Cube object()
 */
class Cube extends Base {

    /**
     * @var Fact[]
     */
    private $facts = [];

    /**
     * @var Type
     */
    private $dataType;

    /**
     * @var UserQuery[]
     */
    private $userQueries;

    /**
     * @param Connection $db
     * @param \OLAP\Cube $object
     * @param Base $sender
     */
    public function __construct(Connection $db, \OLAP\Cube $object, Base $sender = null) {

        parent::__construct($db, $object, $sender);

        $this->facts = [];
        foreach ($this->object()->getFacts() as $fact) {
            $this->facts[$fact->getName()] = new Fact($this->db(), $fact, $this);
        }
        $this->dataType = new Type($this->db(), $this->object()->getDataType());
    }

    /**
     * @return Fact[]
     */
    public function getFacts() {

        return $this->facts;
    }

    /**
     * @param $name
     * @return Fact
     */
    public function getFact($name) {

        return empty($this->facts[$name]) ? false : $this->facts[$name];
    }

    /**
     * @return Type
     */
    public function getDataType() {

        return $this->dataType;
    }

    /**
     * Check structure of cube
     */
    public function checkStructure() {

        $this->getDataType()->checkStructure();

        foreach ($this->getFacts() as $fact) {
            $fact->checkStructure();
        }
    }

    /**
     * @return string
     */
    public function dataField() {

        return $this->object()->dataField();
    }

    /**
     * @return string
     */
    public function valueField() {

        return $this->object()->valueField();
    }

    public function setData(array $data) {

        foreach ($this->getFacts() as $fact) {
            if ($fact->object()->getParent()) {
                // will updated by events
                continue;
            }
            $fact->setData($data);
        }
    }

    /**
     * @param array $data
     * @return UserQuery
     */
    public function getSetter(array $data) {

        return $this->getUserQuery('setter', $this->getDataType()->object()->getSetData(), $data);
    }

    /**
     * @param array $data
     * @return UserQuery
     */
    public function getPusher(array $data) {

        return $this->getUserQuery('pusher', $this->getDataType()->object()->getPushData(), $data);
    }

    /**
     * @return UserQuery
     */
    public function getAggregate() {

        return $this->getUserQuery('aggregate', $this->getDataType()->object()->getAggregate());
    }

    /**
     * @param string $query
     * @param string $str
     * @param array $data
     * @param int $i
     * @return UserQuery
     */
    protected function getUserQuery($query, $str, array $data = [], $i = 0) {

        if (empty($this->userQueries[$query])) {
            $params = [];
            if (preg_match_all('/%([^%]+?)%/i', $str, $matches) && !empty($matches[1])) {
                foreach ($matches[1] as $match) {
                    if ($match === 'DATA_FIELD') {
                        $str = str_replace("%$match%", $this->dataField(), $str);
                    } elseif ($match === 'DATA_TYPE') {
                        $str = str_replace("%$match%", $this->getDataType()->object()->getName(), $str);
                    } elseif (array_key_exists($match, $data)) {
                        $str = str_replace("%$match%", ':param_' . $i, $str);
                        $params[':param_' . $i] = $data[$match];
                        ++$i;
                    }
                }
            }
            $this->userQueries[$query] = new UserQuery($str, $params);
        }
        return $this->userQueries[$query];
    }
}