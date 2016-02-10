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
        $this->dataType = $dataType instanceof Type ? $dataType : new Type($dataType);
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
}