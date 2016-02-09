<?php

namespace OLAP;

class Dimension {

    const PREFIX = 'dimension_';

    private $name = null;
    private $type = null;
    private $options = null;

    public function __construct($name, $type, $options) {

        $this->name = $name;
        $this->type = TypeBuilder::get($type);
        $this->options = $options;
    }

    public function getName() {

        return self::PREFIX . $this->name;
    }
}