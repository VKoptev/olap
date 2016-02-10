<?php

namespace OLAP;


class Cube {

    const PREFIX = 'facts_';

    /**
     * @var string
     */
    private $name;
    /**
     * @var Dimension[]
     */
    private $dimensions = [];

    /**
     * @var Type
     */
    private $dataType;

    private $pusher = ['', []];
    private $setter = ['', []];
    private $aggregate = ['', []];


    public function __construct($name, $dimensions = [], $dataType = []) {

        $this->name = $name;
        foreach ($dimensions as $dimension) {
            if (is_array($dimension) && !empty($dimension['name']) && !empty($dimension['type'])) {
                $dimension = new Dimension(
                    $dimension['name'],
                    $dimension['type'],
                    array_diff_key( $dimension, array_flip(['name', 'type']) )
                );
            }
            if ($dimension instanceof Dimension) {
                $this->dimensions[strtolower($dimension->getName())] = $dimension;
            }
        }
        $this->dataType = $dataType instanceof DataType ? $dataType : new DataType($dataType);
    }

    public function dataField() {

        return 'data';
    }

    public function valueField() {

        return 'value';
    }

    /**
     * @return string
     */
    public function getName() {

        return self::PREFIX . $this->name;
    }

    /**
     * @return Dimension[]
     */
    public function getDimensions() {

        return $this->dimensions;
    }

    /**
     * @return Type
     */
    public function getDataType() {

        return $this->dataType;
    }

    public function getSetter(array $data) {

        if (empty($this->setter[0])) {
            $this->setter = $this->parseFields($this->dataType->getSetData(), $data);
        }
        return $this->setter;
    }

    public function getPusher(array $data) {

        if (empty($this->pusher[0])) {
            $this->pusher = $this->parseFields($this->dataType->getPushData(), $data);
        }
        return $this->pusher;
    }

    public function getAggregate() {

        if (empty($this->aggregate[0])) {
            $this->aggregate = $this->parseFields($this->dataType->getAggregate(), []);
        }
        return $this->aggregate;
    }

    public function parseFields($str, array $data, $i = 0) {

        $params = [];
        if (preg_match_all('/%([^%]+?)%/i', $str, $matches) && !empty($matches[1])) {
            foreach ($matches[1] as $match) {
                if ($match === 'DATA_FIELD') {
                    $str = str_replace("%$match%", $this->dataField(), $str);
                } elseif ($match === 'DATA_TYPE') {
                    $str = str_replace("%$match%", $this->dataType->getType(), $str);
                } elseif (array_key_exists($match, $data)) {
                    $str = str_replace("%$match%", ':param_' . $i, $str);
                    $params[':param_' . $i] = $data[$match];
                    ++$i;
                }
            }
        }
        return [$str, $params];
    }
}