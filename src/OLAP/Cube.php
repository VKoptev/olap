<?php

namespace OLAP;


class Cube extends Model {

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
            if (is_array($fact) && !empty($fact['name'])) {
                if (!empty($fact['dimensions'])) {
                    $fact = new Fact(
                        $fact['name'],
                        $fact['dimensions'],
                        array_merge(
                            array_diff_key($fact, array_flip(['name', 'dimensions'])),
                            ['cube_name' => $this->name]
                        )
                    );
                } elseif (!empty($fact['special'])) {
                    $fact = $this->getSpecialFact($fact);
                }
            }
            if ($fact instanceof Fact) {
                $this->facts[$fact->getName()] = $fact;
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

    protected function getSpecialFact($fact) {

        $class = __NAMESPACE__ . '\\SpecialFact\\' . ucfirst($fact['special']);
        return new $class(
            $fact['name'],
            array_merge(
                array_diff_key($fact, array_flip(['name', 'special'])),
                ['cube_name' => $this->name]
            )
        );
    }
}