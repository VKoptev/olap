<?php

namespace OLAP;


class Cube extends Model {

    /**
     * @var string
     */
    private $name;
    /**
     * @var Fact[]
     */
    private $facts = [];

    /**
     * @var DataType
     */
    private $dataType;

//    private $pusher = ['', []];
//    private $setter = ['', []];
//    private $aggregate = ['', []];


    public function __construct($name, $facts = [], $dataType = [], $options = []) {

        $this->name = $name;
        foreach ($facts as $fact) {
            if (is_array($fact) && !empty($fact['name']) && !empty($fact['dimensions'])) {
                $fact = new Fact(
                    $fact['name'],
                    $fact['dimensions'],
                    array_merge(array_diff_key( $fact, array_flip(['name', 'dimensions']) ), ['cube_name' => $this->name])
                );
            }
            if ($fact instanceof Fact) {
                $this->facts[strtolower($fact->getName())] = $fact;
            } else {
                throw new Exception('Bad cube format');
            }
        }
        $this->dataType = $dataType instanceof DataType ? $dataType : new DataType($dataType);
        $this->options = $options;
    }

    public function dataField() {

        return $this->getOption('data_field', 'data');
    }

    public function valueField() {

        return $this->getOption('value_field', 'value');
    }

    /**
     * @return string
     */
    public function getName() {

        return self::PREFIX . $this->name;
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
     * @return DataType
     */
    public function getDataType() {

        return $this->dataType;
    }

//    public function getSetter(array $data) {
//
//        if (empty($this->setter[0])) {
//            $this->setter = $this->parseFields($this->dataType->getSetData(), $data);
//        }
//        return $this->setter;
//    }
//
//    public function getPusher(array $data) {
//
//        if (empty($this->pusher[0])) {
//            $this->pusher = $this->parseFields($this->dataType->getPushData(), $data);
//        }
//        return $this->pusher;
//    }
//
//    public function getAggregate() {
//
//        if (empty($this->aggregate[0])) {
//            $this->aggregate = $this->parseFields($this->dataType->getAggregate(), []);
//        }
//        return $this->aggregate;
//    }
//
//    public function parseFields($str, array $data, $i = 0) {
//
//        $params = [];
//        if (preg_match_all('/%([^%]+?)%/i', $str, $matches) && !empty($matches[1])) {
//            foreach ($matches[1] as $match) {
//                if ($match === 'DATA_FIELD') {
//                    $str = str_replace("%$match%", $this->dataField(), $str);
//                } elseif ($match === 'DATA_TYPE') {
//                    $str = str_replace("%$match%", $this->dataType->getType(), $str);
//                } elseif (array_key_exists($match, $data)) {
//                    $str = str_replace("%$match%", ':param_' . $i, $str);
//                    $params[':param_' . $i] = $data[$match];
//                    ++$i;
//                }
//            }
//        }
//        return [$str, $params];
//    }
}