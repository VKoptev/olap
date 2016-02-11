<?php

namespace OLAP\SpecialFact;


use OLAP\Fact;

class Timezone extends Fact {

    public function __construct($name, $options = []) {

        $dimensions = [
            [
                'name' => 'timezone',
                'type' => 'character varying(100)'
            ],
            [
                'name' => 'year',
                'type' => 'date',
                'parent' => 'timezone',
                'denormalized' => true,
                'index' => 'btree'
            ],
            [
                'name' => 'month',
                'type' => 'date',
                'parent' => 'year',
                'denormalized' => true,
                'index' => 'btree'
            ],
            [
                'name' => 'day',
                'type' => 'date',
                'parent' => 'month',
                'denormalized' => true,
                'index' => 'btree'
            ],
        ];
        parent::__construct($name, $dimensions, $options);
    }
}