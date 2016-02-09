<?php

namespace OLAP;

class Server {

    /**
     * @var Connection
     */
    private $connection;
    /**
     * @var Dimension[]
     */
    private $dimensions = [];

    /**
     * @var
     */
    private $dataType;

    public function __construct(Connection $connection, $dimensions = [], $dataType = '') {

        $this->connection = $connection;
        foreach ($dimensions as $dimension) {
            if (is_array($dimension) && !empty($dimension['name']) && !empty($dimension['type'])) {
                $dimension = new Dimension($dimension['name'], $dimension['type'], empty($dimension['options']) ? [] : $dimension['options']);
            }
            if ($dimension instanceof Dimension) {
                $this->dimensions[$dimension->getName()] = $dimension;
            }
        }
    }

    public function checkStructure() {

        //
    }
}