<?php

namespace OLAP;


class Fact extends Model {

    const PREFIX = 'fact_';

    /**
     * @var Dimension[]
     */
    private $dimensions = [];

    /**
     * @var Type
     */
    private $dataType;

    public function __construct($name, $dimensions = [], $options = []) {

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
            } else {
                throw new Exception('Bad fact format');
            }
        }
        $this->options = $options;
    }

    /**
     * @return Dimension[]
     */
    public function getDimensions() {

        return $this->dimensions;
    }

    /**
     * @param $name
     * @return Dimension
     */
    public function getDimension($name) {

        return empty($this->dimensions[$name]) ? false : $this->dimensions[$name];
    }

    /**
     * @return Type
     */
    public function getDataType() {

        return $this->dataType;
    }

    public function getParent() {

        $parent = $this->getOption('parent', null);
        return $parent ? $this->makeName($parent) : null;
    }

    public function isSpecial() {

        return $this->getOption('special', false);
    }

    protected function makeName($name) {

        return self::PREFIX . $this->getOption('cube_name', 'cube') . '_' . $name;
    }
}