<?php

namespace OLAP\Event;


class Type {

    const EVENT_SET_DATA = 1;
    const EVENT_SET_ALL_DATA = 2;

    const EVENT_TRUNCATE_FACT = 100;

    static protected $access = [
        self::EVENT_SET_DATA => true,
        self::EVENT_SET_ALL_DATA => true,

        self::EVENT_TRUNCATE_FACT => true,
    ];

    static public function isValid($type) {

        return !empty(self::$access[$type]);
    }
}