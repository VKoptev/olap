<?php

namespace OLAP\Event;


class Type {

    const EVENT_SET_DATA = 1;
    const EVENT_SET_ALL_DATA = 2;

    static protected $access = [
        self::EVENT_SET_DATA => true,
        self::EVENT_SET_ALL_DATA => true,
    ];

    static public function isValid($type) {

        return !empty(self::$access[$type]);
    }
}