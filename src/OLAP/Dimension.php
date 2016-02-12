<?php

namespace OLAP;

class Dimension extends Model {

    const PREFIX = 'dimension_';

    /**
     * @var Type
     */
    private $type;

    public function __construct($name, $type, $options) {

        $this->name = $name;
        $this->type = $type instanceof Type ? $type : new Type($type);
        $this->options = $options ?: [];
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

    public function getParent() {

        return empty($this->options['parent']) ? null : $this->makeName($this->options['parent']);
    }

    public function getIndex() {

        return empty($this->options['index']) ? 'hash' : $this->options['index'];
    }

    /**
     * Return dimension value
     * @param array $data
     * @return mixed
     */
    public function mapValue(array $data) {

        return array_key_exists($this->name, $data) ? $data[$this->name] : null;
    }

    protected function makeName($name) {

        return self::PREFIX . $name;
    }
}