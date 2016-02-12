<?php
namespace OLAP\DB;


class UserQuery {

    /**
     * @var string
     */
    private $query  = '';
    /**
     * @var array
     */
    private $params = [];

    public function __construct($query, $params) {

        $this->query  = $query;
        $this->params = $params;
    }

    /**
     * @return string
     */
    public function getQuery() {

        return $this->query;
    }

    /**
     * @return array
     */
    public function getParams() {

        return $this->params;
    }
}