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

    private $slice = [];
    private $drill = null;

    /**
     * @param Connection $db
     * @param \OLAP\Cube $object
     * @param Base $sender
     */
    public function __construct(Connection $db, \OLAP\Cube $object, Base $sender = null) {

        parent::__construct($db, $object, $sender);

        $defaultDrill = null;
        $this->facts = [];
        foreach ($this->object()->getFacts() as $fact) {
            $class = Fact::class;
            if ($fact->isSpecial()) {
                $class = $this->getSpecialFact($fact);
            }
            if (!$defaultDrill && $fact->isDefaultDrill()) {
                $defaultDrill = $fact->getName();
            }
            $this->facts[$fact->getName()] = new $class($this->db(), $fact, $this);
        }
        $this->drill = $defaultDrill;
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

    public function truncate() {

        foreach ($this->getFacts() as $fact) {
            if ($fact->object()->getParent()) {
                // will updated by events
                continue;
            }
            $fact->truncate();
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
    public function getPusher(array $data) {

        return $this->getUserQuery('pusher', $this->getDataType()->object()->getPushData(), $data);
    }

    /**
     * @param string $alias
     * @return UserQuery
     */
    public function getAggregate($alias = '') {

        return $this->getUserQuery('aggregate', $this->getDataType()->object()->getAggregate(), ['%ALIAS%' => $alias ? "$alias." : '']);
    }

    /**
     * @param string $alias
     * @return UserQuery
     */
    public function getAggregateLinear($alias = '') {

        return $this->getUserQuery('aggregate_linear', $this->getDataType()->object()->getAggregateLinear(), ['%ALIAS%' => $alias ? "$alias." : '']);
    }

    /**
     * @param $slice
     * @return $this
     */
    public function slice($slice) {

        $this->slice = $slice;
        return $this;
    }

    /**
     * @param $drill
     * @return $this
     */
    public function drill($drill) {

        foreach ($this->getFacts() as $fact) {
            if ($fact->isDrill($drill)) {
                $this->drill = $fact->object()->getName();
                break;
            }
        }
        return $this;
    }

    public function get() {

        return $this->getFact($this->drill)->get($this->slice);
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
                        $data['%ALIAS%'] = empty($data['%ALIAS%']) ? '' : $data['%ALIAS%'];
                        $str = str_replace("%$match%", $data['%ALIAS%'] . $this->dataField(), $str);
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

    protected function getSpecialFact($fact) {

        $class = get_class($fact);
        $n = strpos($class, 'SpecialFact\\');
        if ($n === false) {
            $class = Fact::class;
        } else {
            $class = __NAMESPACE__ . '\\' . substr($class, $n);
        }
        return $class;
    }
}