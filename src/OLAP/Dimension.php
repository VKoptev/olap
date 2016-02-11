<?php

namespace OLAP;

class Dimension implements Model {

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

    public function getIndex() {

        return empty($this->options['index']) ? 'hash' : $this->options['index'];
    }

    /**
     * Return dimension value
     * @param array $data
     * @param array $map [dimension_name => data_key]
     * @return mixed
     */
    public function getData(array $data, array $map = []) {

        $key = $this->name;
        if (!empty($map[$key])) {
            $key = $map[$key];
        }
        return array_key_exists($key, $data) ? $data[$key] : null;
    }
}