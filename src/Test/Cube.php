<?php

namespace Test;


class Cube {

    static public function get() {
        return new \OLAP\Cube(
            'tracks',
            [ // facts
                'hour' => [
                    'name' => 'hour',
                    'dimensions' => [
                        [
                            'name' => 'hour',
                            'type' => 'timestamp without time zone',
                            'denormalized' => true,
                            'index' => 'btree',
                        ],
                        [
                            'name' => 'pid',
                            'type' => 'integer',
                        ],
                        [
                            'name' => 'supplier',
                            'type' => 'character varying(255)',
                        ],
                        [
                            'name' => 'offer',
                            'type' => 'character varying(255)',
                            'parent' => 'supplier',
                        ],
                        [
                            'name' => 'country',
                            'type' => 'character varying(5)',
                        ],
                        [
                            'name' => 'city_id',
                            'type' => 'integer',
                            'parent' => 'country',
                        ],
                        [
                            'name' => 'os',
                            'type' => 'character varying(255)',
                        ],
                        [
                            'name' => 'browser',
                            'type' => 'character varying(255)',
                        ],
                        [
                            'name' => 'device',
                            'type' => 'character varying(255)',
                        ],
                        [
                            'name' => 'device_model',
                            'type' => 'character varying(255)',
                            'parent' => 'device',
                        ],
                    ]
                ],
                'date' => [
                    'name' => 'date',
                    'special' => 'timezone',
                    'parent' => 'hour',
                    'dimension' => 'hour',
                ],
                'sub_hour' => [
                    'name' => 'sub_hour',
                    'parent' => 'hour',
                    'dimensions' => [
                        [
                            'name' => 'sub1',
                            'type' => 'character varying(255)',
                        ],
                        [
                            'name' => 'sub2',
                            'type' => 'character varying(255)',
                        ],
                        [
                            'name' => 'sub3',
                            'type' => 'character varying(255)',
                        ],
                        [
                            'name' => 'sub4',
                            'type' => 'character varying(255)',
                        ],
                        [
                            'name' => 'sub5',
                            'type' => 'character varying(255)',
                        ],
                    ],
                ],
            ],
            [
                'name' => 'info',
                'create' => 'CREATE TYPE info AS("raw" integer,uniq integer)',
                'aggregate' => 'Row(SUM((%DATA_FIELD%).raw), SUM((%DATA_FIELD%).uniq))::%DATA_TYPE%',
                'aggregate_linear' => 'SUM((%DATA_FIELD%).raw) as raw, SUM((%DATA_FIELD%).uniq) as uniq',
                'set_data' => '%DATA_FIELD%.raw = %raw%, %DATA_FIELD%.uniq = %uniq%',
                'push_data' => <<<SQL
%DATA_FIELD%.raw = COALESCE((%DATA_FIELD%).raw, 0) + %raw%,
%DATA_FIELD%.uniq = COALESCE((%DATA_FIELD%).uniq, 0) + %uniq%
SQL

            ]
        );
    }
}