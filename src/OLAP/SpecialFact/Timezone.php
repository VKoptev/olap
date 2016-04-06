<?php

namespace OLAP\SpecialFact;


use OLAP\Dimension;
use OLAP\Fact;

class Timezone extends Fact {

    public function __construct($name, $options = []) {

        $dimensions = [
            [
                'name' => 'timezone',
                'type' => 'integer'
            ],
            [
                'name' => 'year',
                'type' => 'date',
                'parent' => 'timezone',
                'index' => 'btree',
                'format' => 'Y',
                'db-format' => 'Y-01-01',
            ],
            [
                'name' => 'month',
                'type' => 'date',
                'parent' => 'year',
                'index' => 'btree',
                'format' => 'Y-m',
                'db-format' => 'Y-m-01',
            ],
            [
                'name' => 'day',
                'type' => 'date',
                'parent' => 'month',
                'index' => 'btree',
                'format' => 'Y-m-d',
                'db-format' => 'Y-m-d',
            ],
        ];
        parent::__construct($name, $dimensions, $options);
    }

    public function getSpecialDimension($name = true) {

        $dimension = $this->getOption('dimension');
        $dimension = $dimension ? new Dimension($dimension, '', []) : null;
        return $dimension ? ($name ? $dimension->getName() : $dimension) : false;
    }
}