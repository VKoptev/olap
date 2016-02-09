<?php

namespace OLAP;

class Dimension {

    const PREFIX = 'dimension_';

    private $name;
    /**
     * @var Type
     */
    private $type;
    private $options = [];

    public function __construct($name, $type, $options = []) {

        $this->name = $name;
        $this->type = $type instanceof Type ? $type : new Type($type);
        $this->options = $options ?: [];
    }

    public function getName() {

        return self::PREFIX . $this->name;
    }

    /**
     * @return Type
     */
    public function getType() {

        return $this->type;
    }

    public function isDenormalized() {

        return !empty($this->options['denormalized']);
    }
}